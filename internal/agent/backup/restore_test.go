package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRestoreArgsGolden(t *testing.T) {
	s := Settings{Stanza: "demo-catalog-pg18", Repo: Repo{Type: TypePosix, Path: "/backups"}}
	cases := []struct {
		name string
		opts RestoreOptions
	}{
		{"default", RestoreOptions{}},
		{"default-set", RestoreOptions{BackupID: "20260819-100000F"}},
		{"immediate", RestoreOptions{Type: TargetImmediate, BackupID: "20260819-100000F"}},
		{"time", RestoreOptions{Type: TargetTime, Target: "2026-08-19 10:15:00+00"}},
		{"time-exclusive-tli", RestoreOptions{Type: TargetTime, Target: "2026-08-19 10:15:00+00", Exclusive: true, TargetTLI: 3}},
		{"lsn", RestoreOptions{Type: TargetLSN, Target: "0/3000060", TargetTLI: 2}},
		{"name-source-stanza", RestoreOptions{Type: TargetName, Target: "before-purge", BackupID: "20260819-100000F_20260819-101000I", Stanza: "src-catalog-pg18"}},
		{"xid-exclusive", RestoreOptions{Type: TargetXID, Target: "1234", BackupID: "20260819-100000F", Exclusive: true}},
		{"standby-delta", RestoreOptions{Type: TargetStandby, Delta: true}},
	}
	var b strings.Builder
	for _, c := range cases {
		args, err := RestoreArgs(s, c.opts)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		b.WriteString(c.name + ": " + strings.Join(args, " ") + "\n")
		for k, v := range RecoverySettings(c.opts) {
			if !contains(RecoverySettingNames, k) {
				t.Errorf("%s: recovery setting %s not in RecoverySettingNames", c.name, k)
			}
			_ = v
		}
		b.WriteString("  recovery: " + settingsLine(RecoverySettings(c.opts)) + "\n")
	}
	golden := filepath.Join("testdata", "restore-args.txt")
	if *update {
		if err := os.WriteFile(golden, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if b.String() != string(want) {
		t.Errorf("golden mismatch\n--- got\n%s\n--- want\n%s", b.String(), want)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func settingsLine(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, " ")
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func TestRestoreOptionsValidate(t *testing.T) {
	bad := map[string]RestoreOptions{
		"name without set":       {Type: TargetName, Target: "rp"},
		"xid without set":        {Type: TargetXID, Target: "42"},
		"immediate without set":  {Type: TargetImmediate},
		"immediate with target":  {Type: TargetImmediate, BackupID: "x", Target: "y"},
		"time without target":    {Type: TargetTime},
		"lsn without target":     {Type: TargetLSN},
		"default with target":    {Target: "x"},
		"standby with target":    {Type: TargetStandby, Target: "x"},
		"exclusive on default":   {Exclusive: true},
		"exclusive on immediate": {Type: TargetImmediate, BackupID: "x", Exclusive: true},
		"negative timeline":      {Type: TargetTime, Target: "t", TargetTLI: -1},
		"unknown type":           {Type: "bogus"},
	}
	for name, o := range bad {
		if err := o.Validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
		if _, err := RestoreArgs(Settings{Stanza: "s"}, o); err == nil {
			t.Errorf("%s: RestoreArgs accepted invalid options", name)
		}
	}
	good := []RestoreOptions{
		{}, {Type: TargetDefault, BackupID: "x"}, {Type: TargetStandby, Delta: true},
		{Type: TargetTime, Target: "t", Exclusive: true}, {Type: TargetLSN, Target: "0/1"},
		{Type: TargetName, Target: "n", BackupID: "x"}, {Type: TargetXID, Target: "1", BackupID: "x", TargetTLI: 4},
		{Type: TargetImmediate, BackupID: "x"},
	}
	for _, o := range good {
		if err := o.Validate(); err != nil {
			t.Errorf("%s: unexpected error %v", o, err)
		}
	}
}

func TestRunnerRestoreAndHasCompletedBackup(t *testing.T) {
	info, _ := os.ReadFile("testdata/info.json")
	r := NewRunner(Settings{Stanza: "t-catalog-pg18", Repo: Repo{Type: TypePosix, Path: "/b"}}, nil)
	var got []string
	r.Exec = func(_ context.Context, args []string, onLine func(string)) error {
		got = args
		if args[len(args)-1] == "info" {
			onLine(string(info))
			return nil
		}
		onLine("P00   INFO: restore command end: completed successfully")
		return nil
	}
	tail, err := r.Restore(context.Background(), RestoreOptions{Type: TargetStandby, Delta: true})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := RestoreArgs(r.Settings, RestoreOptions{Type: TargetStandby, Delta: true})
	if !reflect.DeepEqual(got, want) || len(tail) != 1 {
		t.Fatalf("args %v tail %v", got, tail)
	}
	ok, err := r.HasCompletedBackup(context.Background())
	if err != nil || !ok {
		t.Fatalf("HasCompletedBackup = %v, %v", ok, err)
	}
	r.Exec = func(_ context.Context, _ []string, onLine func(string)) error {
		onLine("P00  ERROR: [040]: unable to find backup set")
		return errors.New("exit status 40")
	}
	if _, err := r.Restore(context.Background(), RestoreOptions{}); err == nil || !strings.Contains(err.Error(), "[040]") {
		t.Fatalf("restore error = %v", err)
	}
	if _, err := r.Restore(context.Background(), RestoreOptions{Type: TargetName, Target: "x"}); err == nil {
		t.Fatal("invalid options accepted")
	}
}

func TestRenderExtraStanza(t *testing.T) {
	got, err := Render(Settings{Stanza: "new-catalog-pg18", Repo: Repo{Type: TypePosix, Path: "/b"}}, "/pgdata", 5432, "old-catalog-pg18")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "\n[new-catalog-pg18]\npg1-path=/pgdata\n") || !strings.Contains(got, "\n[old-catalog-pg18]\npg1-path=/pgdata\n") {
		t.Fatalf("missing stanza sections:\n%s", got)
	}
	if strings.Index(got, "[new-catalog-pg18]") > strings.Index(got, "[old-catalog-pg18]") {
		t.Fatal("own stanza must come first")
	}
	if RestoreCommandFor(Settings{Stanza: "new"}, "old-catalog-pg18") != `pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=old-catalog-pg18 archive-get %f "%p"` {
		t.Fatal(RestoreCommandFor(Settings{Stanza: "new"}, "old-catalog-pg18"))
	}
}
