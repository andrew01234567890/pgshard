package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const name = "pgshard-router"

func run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := Run(name, args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestHelp(t *testing.T) {
	code, out, errs := run(t, "--help")
	want := descriptions[name] + "\n\nUsage: pgshard-router [--help | --version]\n"
	if code != 0 || out != want || errs != "" {
		t.Fatalf("got code=%d out=%q err=%q", code, out, errs)
	}
}

func TestVersion(t *testing.T) {
	code, out, errs := run(t, "--version")
	if code != 0 || out != "pgshard-router dev (none, unknown)\n" || errs != "" {
		t.Fatalf("got code=%d out=%q err=%q", code, out, errs)
	}
}

func TestUnknownFlag(t *testing.T) {
	code, out, errs := run(t, "--bogus")
	want := "pgshard-router: unknown argument \"--bogus\"\nUsage: pgshard-router [--help | --version]\n"
	if code != 2 || out != "" || errs != want {
		t.Fatalf("got code=%d out=%q err=%q", code, out, errs)
	}
}

func TestExtraArgsAfterHelpIsUsageError(t *testing.T) {
	code, _, _ := run(t, "--help", "extra")
	if code != 2 {
		t.Fatalf("code = %d, want 2", code)
	}
}

func TestNoArgs(t *testing.T) {
	code, out, errs := run(t)
	if code != 1 || out != "" || errs != "pgshard-router: runtime services are not implemented yet\n" {
		t.Fatalf("got code=%d out=%q err=%q", code, out, errs)
	}
}

func TestUnknownCommand(t *testing.T) {
	var out, errb bytes.Buffer
	if code := Run("nope", nil, &out, &errb); code != 2 || errb.String() != "nope: unknown command\n" {
		t.Fatalf("code=%d err=%q", code, errb.String())
	}
}

func TestEveryCommandHasDescription(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("..", "..", "cmd"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(descriptions) {
		t.Fatalf("cmd/ has %d entries, descriptions has %d", len(entries), len(descriptions))
	}
	for _, e := range entries {
		if _, ok := descriptions[e.Name()]; !ok {
			t.Errorf("cmd/%s has no description", e.Name())
		}
	}
}

func TestRealBinary(t *testing.T) {
	bin := filepath.Join(t.TempDir(), name)
	build := exec.Command("go", "build",
		"-ldflags", "-X github.com/andrew01234567890/pgshard/internal/buildinfo.Version=9.9.9 -X github.com/andrew01234567890/pgshard/internal/buildinfo.Commit=cafe -X github.com/andrew01234567890/pgshard/internal/buildinfo.Date=today",
		"-o", bin, "../../cmd/"+name)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cases := []struct {
		args []string
		code int
		out  string
		err  string
	}{
		{[]string{"--version"}, 0, "pgshard-router 9.9.9 (cafe, today)\n", ""},
		{[]string{"--help"}, 0, descriptions[name] + "\n\nUsage: pgshard-router [--help | --version]\n", ""},
		{[]string{"--nope"}, 2, "", "pgshard-router: unknown argument \"--nope\"\nUsage: pgshard-router [--help | --version]\n"},
		{nil, 1, "", "pgshard-router: runtime services are not implemented yet\n"},
	}
	for _, c := range cases {
		var out, errb bytes.Buffer
		cmd := exec.Command(bin, c.args...)
		cmd.Stdout, cmd.Stderr = &out, &errb
		err := cmd.Run()
		code := 0
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		if code != c.code || out.String() != c.out || errb.String() != c.err {
			t.Errorf("args=%v: code=%d out=%q err=%q", c.args, code, out.String(), errb.String())
		}
	}
}

func TestSubcommandDispatch(t *testing.T) {
	var out, errb bytes.Buffer
	sub := map[string]Subcommand{"serve": func(args []string, stdout, _ io.Writer) int {
		fmt.Fprintf(stdout, "serve %v", args)
		return 7
	}}
	if code := RunWith(name, []string{"serve", "--x", "1"}, &out, &errb, sub); code != 7 || out.String() != "serve [--x 1]" {
		t.Fatalf("code=%d out=%q", code, out.String())
	}
	out.Reset()
	if code := RunWith(name, []string{"--help"}, &out, &errb, sub); code != 0 || !strings.HasPrefix(out.String(), descriptions[name]) {
		t.Fatalf("--help with subcommands: code=%d out=%q", code, out.String())
	}
	if code := RunWith(name, []string{"other"}, &out, &errb, sub); code != 2 {
		t.Fatalf("unknown subcommand code=%d", code)
	}
}
