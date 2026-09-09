package dockertest

import (
	"net"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestAReadinessFailureSaysWhetherTheMappingWorked: the log says whether
// the server started, and nothing said whether the way to reach it exists.
// PGS-711 is repeatedly the second: PostgreSQL logs that it is listening on
// both address families, is ready, and the host gets connection refused on
// the mapped port for the whole wait. Without this the message sends the
// reader to the container log, which is the wrong half.
func TestAReadinessFailureSaysWhetherTheMappingWorked(t *testing.T) {
	p := probes(true, "running", steady("no progress at all"))
	p.ports = func(string) string {
		return "published ports:\n  5432/tcp -> 127.0.0.1:1  ->  NOTHING is listening on the host"
	}
	err := waitReady("c", refused, p, 50*time.Millisecond, time.Minute)
	if err == nil {
		t.Fatal("it never gave up")
	}
	if !strings.Contains(err.Error(), "NOTHING is listening on the host") {
		t.Fatalf("the failure does not say whether the mapping worked:\n%s", err)
	}
}

// TestHostListeningTellsTheTwoCasesApart is the discriminator itself,
// against real sockets: a port something is listening on and a port
// nothing is.
func TestHostListeningTellsTheTwoCasesApart(t *testing.T) {
	if _, err := exec.LookPath("ss"); err != nil {
		t.Skip("ss is not on PATH, so this cannot tell the cases apart")
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()
	live := lis.Addr().(*net.TCPAddr).Port

	// A port nothing holds: bind one and let it go, so the number is known
	// to be free rather than guessed.
	spare, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := spare.Addr().(*net.TCPAddr).Port
	if err := spare.Close(); err != nil {
		t.Fatal(err)
	}

	if got := hostListening(mapping(live)); !strings.Contains(got, "host IS listening") {
		t.Fatalf("a port with a listener reported %q", got)
	}
	if got := hostListening(mapping(dead)); !strings.Contains(got, "NOTHING is listening") {
		t.Fatalf("a port with no listener reported %q", got)
	}
	if got := hostListening("nonsense"); got != "unparsed" {
		t.Fatalf("an unparseable mapping reported %q", got)
	}
}

func mapping(port int) string {
	return "5432/tcp -> 127.0.0.1:" + strconv.Itoa(port)
}
