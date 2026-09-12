package dockertest

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
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

	// connectEvery is how often the server is asked, and probeEvery how
	// often docker is. They differ by an order of magnitude because their
	// costs do.
	connectEvery = 100 * time.Millisecond
	probeEvery   = time.Second
)

// Connector reports whether the server is accepting connections. It is a
// parameter so this package does not depend on a driver: every caller
// already has one.
type Connector func(ctx context.Context) error

// WaitReady blocks until connect succeeds, and fails the test when the
// container stops, stops making progress, or reaches ReadyCap.
func WaitReady(tb testing.TB, id string, connect Connector) {
	tb.Helper()
	if err := waitReady(id, connect, liveProbes(), ReadyIdle, ReadyCap); err != nil {
		tb.Fatal(err)
	}
}

func liveProbes() dockerProbes {
	return dockerProbes{running: containerRunning, mark: containerLogMark, log: containerLog, ports: containerPorts}
}

// ErrPortMapping reports the failure that is not the container's: it came
// up, it is running, and nothing on the host is listening on the port
// docker published for it.
//
// The host port is chosen by the caller -- listen on :0, take the number,
// close the listener, hand it to docker -- so between the close and
// docker's bind anything else on the machine can take it. That is a race
// against everything else starting a container at the same moment, which
// is why it fires on a busy run and never in isolation.
var ErrPortMapping = errors.New("the container's published port has nothing listening on the host: the mapping failed, not the server")

// StartAndWait starts a container and waits for it, starting ANOTHER when
// the failure was the port mapping.
//
// Waiting longer cannot help that case and the wait is 90 seconds, so a run
// that hits it pays a minute and a half per container to learn nothing. A
// fresh port is the whole fix, and start already picks one.
//
// Any other failure is the container's own and fails the test at once: a
// container that died in initdb will die again, and retrying it would turn
// one clear error into three.
func StartAndWait(tb testing.TB, attempts int, start func() (id string, connect Connector)) string {
	tb.Helper()
	if attempts < 1 {
		attempts = 1
	}
	for attempt := 1; ; attempt++ {
		id, connect := start()
		err := waitReady(id, connect, liveProbes(), ReadyIdle, ReadyCap)
		if err == nil {
			return id
		}
		if !errors.Is(err, ErrPortMapping) || attempt >= attempts {
			tb.Fatal(err)
		}
		tb.Logf("attempt %d: %v\nstarting another container on a fresh port", attempt, err)
		_ = exec.Command("docker", "rm", "-f", id).Run()
	}
}

// dockerProbes is what waitReady asks about the container. It is a
// parameter so the decisions below can be tested without a docker daemon:
// every branch here is one this package exists to get right, and none of
// them is reachable from a test that needs a real slow container.
type dockerProbes struct {
	running func(id string) (bool, string)
	mark    func(id string) string
	log     func(id string) string
	// ports answers the question the log cannot: whether the failure is
	// the server or the way to reach it.
	ports func(id string) string
}

// waitReady is WaitReady's decision, separated so it can be tested.
//
// It gives up EARLY when the container has exited: a container that died in
// initdb answers nothing, and waiting the full bound for it turns a
// one-line configuration error into a timeout that says only that
// PostgreSQL never became ready.
func waitReady(id string, connect Connector, probe dockerProbes, idle, limit time.Duration) error {
	start := time.Now()
	// The first probe happens on the first pass, so a container that is
	// already gone is reported at once rather than after an interval.
	lastProgress, lastMark, lastProbe := start, probe.mark(id), start.Add(-probeEvery)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := connect(ctx)
		cancel()
		if err == nil {
			return nil
		}
		// Connecting is a local socket; asking docker is two processes and
		// a daemon round trip. Doing both at the connect interval put a
		// hundred docker calls a second on a machine that is already the
		// reason this wait exists, so the loop's own cost lengthened the
		// startup it was measuring. Ask docker on its own, slower clock.
		if time.Since(lastProbe) < probeEvery {
			time.Sleep(connectEvery)
			continue
		}
		lastProbe = time.Now()
		if running, why := probe.running(id); !running {
			return fmt.Errorf("the container stopped after %s without accepting a connection (%s); last error %w\ncontainer log:\n%s",
				time.Since(start).Round(time.Second), why, err, probe.log(id))
		}
		if mark := probe.mark(id); mark != lastMark {
			lastProgress, lastMark = time.Now(), mark
		}
		switch {
		case time.Since(lastProgress) > idle:
			// The ports are probed ONCE and the same answer decides both
			// what the message says and whether this is retryable.
			// Probing again to classify asked docker a second question a
			// moment later, and the container is being torn down around
			// it: the message said the mapping had failed while the
			// classification saw something else, and no retry ever fired.
			mapped := ports(probe, id)
			return mappingOr(mapped, fmt.Errorf("the container logged nothing for %s and never accepted a connection (%s in total); last error %w\n%s\ncontainer log:\n%s",
				idle, time.Since(start).Round(time.Second), err, mapped, probe.log(id)))
		case time.Since(start) > limit:
			mapped := ports(probe, id)
			return mappingOr(mapped, fmt.Errorf("the container did not accept a connection within %s, though it was still logging; last error %w\n%s\ncontainer log:\n%s",
				limit, err, mapped, probe.log(id)))
		}
		time.Sleep(connectEvery)
	}
}

// mappingOr marks err as ErrPortMapping when the ports probe says the host
// side never came up. The message is unchanged either way -- what changes
// is whether a caller may start another container instead of giving up.
func mappingOr(mapped string, err error) error {
	if strings.Contains(mapped, noHostListener) {
		return fmt.Errorf("%w\n%w", ErrPortMapping, err)
	}
	return err
}

// ports is probe.ports with a default, so a caller that did not set one --
// every existing test -- still gets a message rather than a panic.
func ports(probe dockerProbes, id string) string {
	if probe.ports == nil {
		return "published ports: (not probed)"
	}
	return probe.ports(id)
}

// containerPorts reports how docker published the container's ports and
// whether anything on the host is listening on what it published.
//
// It is here because the log cannot answer the question that matters when
// a container starts cleanly and is still unreachable. Seen repeatedly
// (PGS-711): PostgreSQL logs "listening on IPv4 address 0.0.0.0, port
// 5432", is ready, and the host gets connection refused on the mapped port
// for the whole wait. That is not a slow server, and every minute spent
// reading its log is a minute spent on the wrong half of the problem.
//
// A published port that no host socket is listening on means the mapping,
// not the server -- docker-proxy, or a collision on the ephemeral port
// docker chose. Both are worth retrying with a fresh port rather than
// waiting longer.
func containerPorts(id string) string {
	out, err := exec.Command("docker", "port", id).CombinedOutput()
	mapped := strings.TrimSpace(string(out))
	if err != nil || mapped == "" {
		return "published ports: none (docker port said " + strconv.Quote(mapped) + ")"
	}
	var b strings.Builder
	b.WriteString("published ports:\n")
	for _, line := range strings.Split(mapped, "\n") {
		b.WriteString("  " + line + "  ->  " + hostListening(line) + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// hostListening reports whether a host socket is listening on the port a
// `docker port` line published.
func hostListening(mapping string) string {
	_, hostAddr, ok := strings.Cut(mapping, " -> ")
	if !ok {
		return "unparsed"
	}
	_, port, ok := strings.Cut(strings.TrimSpace(hostAddr), ":")
	if !ok {
		return "unparsed"
	}
	out, err := exec.Command("ss", "-ltn").CombinedOutput()
	if err != nil {
		return "host listener unknown (ss unavailable)"
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, ":"+port+" ") {
			return "host IS listening"
		}
	}
	// The distinguishing case, and the reason this exists at all.
	return noHostListener
}

// noHostListener is the phrase that says the failure is docker's mapping
// rather than the server. StartAndWait keys its retry on it, so it is a
// constant rather than a string written twice.
const noHostListener = "NOTHING is listening on the host: the mapping failed, not the server"

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

// containerLogMark is the progress signal: a container that is still
// writing is still starting, however slowly the machine is running it.
//
// It is the last line WITH ITS TIMESTAMP rather than the size of the whole
// log, so it costs the same on a container that has logged a megabyte as
// on one that has logged a line, and so that a line repeated verbatim
// still reads as progress.
func containerLogMark(id string) string {
	out, err := exec.Command("docker", "logs", "--timestamps", "--tail", "1", id).CombinedOutput()
	if err != nil {
		return "unreadable"
	}
	return string(out)
}
