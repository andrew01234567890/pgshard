package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/cli"
)

func TestRunRequiresTLSUnlessInsecureDev(t *testing.T) {
	var out, errb bytes.Buffer
	if code := runPooler(context.Background(), []string{"--listen", "127.0.0.1:0"}, &out, &errb); code != cli.ExitUsage {
		t.Fatalf("code = %d, stderr = %s", code, errb.String())
	}
	if !strings.Contains(errb.String(), "--tls-cert, --tls-key and --tls-ca are required") {
		t.Fatalf("stderr = %s", errb.String())
	}
	errb.Reset()
	if code := runPooler(context.Background(), []string{"--insecure-dev", "--tls-ca", "x"}, &out, &errb); code != cli.ExitUsage {
		t.Fatalf("code = %d", code)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestRunInsecureDevServesAndDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out, errb := &syncBuffer{}, &syncBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- runPooler(ctx, []string{"--insecure-dev", "--listen", "127.0.0.1:0", "--drain-timeout", "1s"}, out, errb)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(out.String(), "listening on") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(out.String(), "INSECURE plaintext") {
		t.Fatalf("stdout = %s", out.String())
	}
	cancel()
	select {
	case code := <-done:
		if code != cli.ExitOK {
			t.Fatalf("code = %d, stderr = %s", code, errb.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not exit")
	}
}

func TestHelpAndVersionStillWork(t *testing.T) {
	var out, errb bytes.Buffer
	if code := cli.RunWith("pgshard-pooler", []string{"--help"}, &out, &errb, map[string]cli.Subcommand{"run": run}); code != cli.ExitOK {
		t.Fatal(code)
	}
	if code := cli.RunWith("pgshard-pooler", []string{"--version"}, &out, &errb, map[string]cli.Subcommand{"run": run}); code != cli.ExitOK {
		t.Fatal(code)
	}
}
