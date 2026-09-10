package dockertest

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestAFailedMappingIsToldApartFromAFailedContainer.
//
// The host port is chosen by the caller -- listen on :0, take the number,
// close the listener, hand it to docker -- so between the close and
// docker's bind anything else on the machine can take it. It fires on a
// busy run and never in isolation: five tests in one run of main, a
// different five in the next, every one passing on its own.
//
// Waiting cannot fix it and the wait is ninety seconds, so every one costs
// a minute and a half to learn nothing. Starting another container on a
// fresh port is the whole fix -- but only for THIS failure. A container
// that died in initdb will die again, and retrying it would turn one clear
// error into three.
func TestAFailedMappingIsToldApartFromAFailedContainer(t *testing.T) {
	mapping := probes(true, "running", steady("no progress at all"))
	mapping.ports = func(string) string { return "published ports:\n  5432/tcp -> 127.0.0.1:1  ->  " + noHostListener }
	err := waitReady("c", refused, mapping, 20*time.Millisecond, time.Minute)
	if !errors.Is(err, ErrPortMapping) {
		t.Fatalf("a container nothing is listening for is not reported as a mapping failure: %v", err)
	}
	// The message a reader gets is unchanged; only what a caller may do
	// about it is.
	if !strings.Contains(err.Error(), noHostListener) {
		t.Fatalf("the failure stopped saying what it is:\n%s", err)
	}

	// The server's own failures are not retryable and must not be marked.
	reachable := probes(true, "running", steady("no progress at all"))
	reachable.ports = func(string) string { return "published ports:\n  5432/tcp -> 127.0.0.1:1  ->  host IS listening" }
	if err := waitReady("c", refused, reachable, 20*time.Millisecond, time.Minute); errors.Is(err, ErrPortMapping) {
		t.Fatalf("a server that is reachable and not answering was marked retryable: %v", err)
	}

	// The answer that decides retryability must be the SAME answer the
	// message reported. Probing again to classify asked docker a second
	// question a moment later, while the container was being torn down
	// around it -- so the message said the mapping had failed, the
	// classification saw something else, and no retry fired. Seen in a real
	// run: three "NOTHING is listening" failures and no retry attempted.
	once := probes(true, "running", steady("no progress at all"))
	calls := 0
	once.ports = func(string) string {
		calls++
		if calls == 1 {
			return "published ports:\n  5432/tcp -> 127.0.0.1:1  ->  " + noHostListener
		}
		return "published ports: none (docker port said \"\")"
	}
	err = waitReady("c", refused, once, 20*time.Millisecond, time.Minute)
	if !errors.Is(err, ErrPortMapping) {
		t.Fatalf("the failure was classified from a second probe rather than the one it reported: %v", err)
	}

	// A container that stopped is its own error, whatever the ports say:
	// it will stop again.
	stopped := probes(false, "exited exit=1", steady("x"))
	stopped.ports = func(string) string { return noHostListener }
	if err := waitReady("c", refused, stopped, time.Minute, time.Minute); errors.Is(err, ErrPortMapping) {
		t.Fatalf("a container that died was marked retryable: %v", err)
	}
}
