package agent

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCommandUsageErrors(t *testing.T) {
	var out, errb bytes.Buffer
	if code := RunCommand(nil, &out, &errb); code != 2 || !strings.Contains(errb.String(), "--config is required") {
		t.Fatalf("code=%d err=%q", code, errb.String())
	}
	errb.Reset()
	if code := RunCommand([]string{"--config", "/nonexistent.json"}, &out, &errb); code != 2 {
		t.Fatalf("code=%d err=%q", code, errb.String())
	}
	errb.Reset()
	if code := RunCommand([]string{"--bogus"}, &out, &errb); code != 2 {
		t.Fatalf("code=%d", code)
	}
	if code := RunCommand([]string{"-h"}, &out, &errb); code != 0 {
		t.Fatalf("help code=%d", code)
	}
	bad := filepath.Join(t.TempDir(), "c.json")
	_ = os.WriteFile(bad, []byte(`{"role":"primary"}`), 0o600)
	errb.Reset()
	if code := RunCommand([]string{"--config", bad}, &out, &errb); code != 2 || !strings.Contains(errb.String(), "cluster is required") {
		t.Fatalf("code=%d err=%q", code, errb.String())
	}
}
