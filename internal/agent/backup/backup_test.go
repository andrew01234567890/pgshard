package backup

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite golden files")

func credDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for k, v := range files {
		if err := os.WriteFile(filepath.Join(dir, k), []byte(v+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestRenderGoldens(t *testing.T) {
	no := false
	cases := map[string]Settings{
		"s3": {Stanza: "demo-catalog-pg18", RetentionFull: 3, RetentionDiff: 4, Repo: Repo{
			Type: TypeS3, Bucket: "pgshard", Endpoint: "http://minio.objectstores.svc:9000", Region: "us-east-1",
			URIStyle: "path", VerifyTLS: &no, Path: "/demo",
			CredentialsDir: credDir(t, map[string]string{CredS3Key: "minioadmin", CredS3KeySecret: "miniosecret"})}},
		"s3-web-id": {Stanza: "demo-shard-0-pg19", Repo: Repo{
			Type: TypeS3, Bucket: "pgshard", Endpoint: "s3.eu-west-1.amazonaws.com", Region: "eu-west-1", KeyType: "web-id", CAFile: "/etc/ssl/ca.pem"}},
		"azure": {Stanza: "demo-catalog-pg18", LogLevel: "detail", Repo: Repo{
			Type: TypeAzure, Bucket: "pgshard", Endpoint: "http://azurite.objectstores.svc:10000", URIStyle: "path", VerifyTLS: &no,
			CredentialsDir: credDir(t, map[string]string{CredAzureAccount: "devstoreaccount1", CredAzureKey: "Zm9v"})}},
		"azure-sas": {Stanza: "demo-catalog-pg18", Repo: Repo{
			Type: TypeAzure, Bucket: "pgshard", KeyType: "sas",
			CredentialsDir: credDir(t, map[string]string{CredAzureAccount: "acct", CredAzureKey: "sv=2020&sig=x"})}},
		"gcs": {Stanza: "demo-catalog-pg18", Repo: Repo{
			Type: TypeGCS, Bucket: "pgshard", Endpoint: "http://fake-gcs.objectstores.svc:4443", VerifyTLS: &no,
			CredentialsDir: "/etc/pgshard-backup-credentials"}},
		"gcs-auto": {Stanza: "demo-catalog-pg18", Repo: Repo{Type: TypeGCS, Bucket: "pgshard", KeyType: "auto"}},
		"gcs-token": {Stanza: "demo-catalog-pg18", Repo: Repo{Type: TypeGCS, Bucket: "pgshard", KeyType: "token", Endpoint: "http://fake-gcs.objectstores.svc:4443",
			CredentialsDir: credDir(t, map[string]string{CredGCSToken: "fake-token"})}},
		"posix": {Stanza: "demo-catalog-pg18", Repo: Repo{Type: TypePosix, Path: "/backups"}},
		"sftp": {Stanza: "demo-catalog-pg18", Repo: Repo{
			Type: TypeSFTP, Host: "sftp.example.internal", HostUser: "backup", HostPort: 2222, HostKeyCheck: "none",
			CredentialsDir: "/etc/pgshard-backup-credentials"}},
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := Render(s, "/var/lib/postgresql/data/pgdata", 5432)
			if err != nil {
				t.Fatal(err)
			}
			got = strings.ReplaceAll(got, s.Repo.CredentialsDir, "CREDDIR")
			golden := filepath.Join("testdata", name+".conf")
			if *update {
				if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Errorf("golden %s mismatch\n--- got\n%s\n--- want\n%s", golden, got, want)
			}
		})
	}
}

func TestRenderEncryptionAndValidation(t *testing.T) {
	pass := filepath.Join(t.TempDir(), "passphrase")
	if err := os.WriteFile(pass, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Render(Settings{Stanza: "x-catalog-pg18", CipherPassFile: pass, Repo: Repo{Type: TypePosix}}, "/pgdata", 5432)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"repo1-cipher-type=aes-256-cbc\n", "repo1-cipher-pass=s3cret\n", "repo1-path=/pgshard\n", "repo1-retention-full=2\n", "[x-catalog-pg18]\npg1-path=/pgdata\npg1-port=5432\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	bad := []Settings{
		{Repo: Repo{Type: TypePosix}},
		{Stanza: "s", Repo: Repo{Type: "ftp"}},
		{Stanza: "s", Repo: Repo{Type: TypeS3, Bucket: "b"}},
		{Stanza: "s", Repo: Repo{Type: TypeS3, Bucket: "b", Endpoint: "e", Region: "r"}},
		{Stanza: "s", Repo: Repo{Type: TypeAzure, Bucket: "b"}},
		{Stanza: "s", Repo: Repo{Type: TypeGCS}},
		{Stanza: "s", Repo: Repo{Type: TypeGCS, Bucket: "b"}},
		{Stanza: "s", Repo: Repo{Type: TypeGCS, Bucket: "b", KeyType: "token"}},
		{Stanza: "s", Repo: Repo{Type: TypeSFTP, Host: "h"}},
		{Stanza: "s", Repo: Repo{Type: TypePosix, Path: "relative"}},
	}
	for i, s := range bad {
		if _, err := Render(s, "/pgdata", 5432); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
	if _, err := Render(Settings{Stanza: "s", Repo: Repo{Type: TypeS3, Bucket: "b", Endpoint: "e", Region: "r", CredentialsDir: t.TempDir()}}, "/pgdata", 5432); err == nil {
		t.Error("expected missing credential file error")
	}
}

func TestCommandsAndStanza(t *testing.T) {
	s := Settings{Stanza: "demo-shard-0-pg18", Repo: Repo{Type: TypePosix}}
	if got := ArchiveCommand(s); got != "pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=demo-shard-0-pg18 archive-push %p" {
		t.Errorf("archive_command: %q", got)
	}
	if got := RestoreCommand(s); got != `pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=demo-shard-0-pg18 archive-get %f "%p"` {
		t.Errorf("restore_command: %q", got)
	}
	if got := StanzaName("demo", "shard-0", 19); got != "demo-shard-0-pg19" {
		t.Errorf("stanza: %q", got)
	}
}

func TestWriteConfig(t *testing.T) {
	root := t.TempDir()
	s := Settings{Stanza: "s", Repo: Repo{Type: TypePosix}, ConfigPath: filepath.Join(root, "etc", "pgbackrest.conf"),
		SpoolPath: filepath.Join(root, "spool"), LogPath: filepath.Join(root, "log")}
	if err := WriteConfig(s, "/pgdata", 5432); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(s.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v", fi.Mode())
	}
	for _, d := range []string{s.SpoolPath, s.LogPath} {
		if _, err := os.Stat(d); err != nil {
			t.Error(err)
		}
	}
}

func TestParseInfo(t *testing.T) {
	b, err := os.ReadFile("testdata/info.json")
	if err != nil {
		t.Fatal(err)
	}
	st, err := ParseInfo(b, "t-catalog-pg18")
	if err != nil {
		t.Fatal(err)
	}
	if st.StatusCode != 0 || st.StatusMessage != "ok" {
		t.Errorf("status %d %q", st.StatusCode, st.StatusMessage)
	}
	if st.ArchiveMin != "000000010000000000000001" || st.ArchiveMax != "000000010000000000000004" {
		t.Errorf("archive range %q..%q", st.ArchiveMin, st.ArchiveMax)
	}
	if len(st.Backups) != 2 {
		t.Fatalf("backups %+v", st.Backups)
	}
	full, incr := st.Backups[0], st.Backups[1]
	if full.Label != "20260819-030047F" || full.Type != "full" || full.Prior != "" || full.StartLSN != 0x2000028 || full.StopLSN != 0x2000120 ||
		full.ArchiveStart != "000000010000000000000002" || full.SizeBytes != 23653062 || full.RepoBytes != 3065814 || full.StartedAt != 1787108447 || full.FinishedAt != 1787108448 {
		t.Errorf("full: %+v", full)
	}
	if incr.Type != "incr" || incr.Prior != "20260819-030047F" || incr.StopLSN != 0x4000050 || incr.ArchiveStop != "000000010000000000000004" || incr.RepoBytes != 394 {
		t.Errorf("incr: %+v", incr)
	}
	if _, err := ParseInfo(b, "other"); err == nil {
		t.Error("expected unknown stanza error")
	}
	if _, err := ParseInfo([]byte("nope"), "x"); err == nil {
		t.Error("expected parse error")
	}
	if _, err := ParseLSN("zz"); err == nil {
		t.Error("expected lsn error")
	}
}

type fakeExec struct {
	calls  [][]string
	fail   map[string]error
	output map[string][]string
}

func (f *fakeExec) run(_ context.Context, args []string, onLine func(string)) error {
	f.calls = append(f.calls, args)
	cmd := args[len(args)-1]
	for _, l := range f.output[cmd] {
		onLine(l)
	}
	return f.fail[cmd]
}

func newRunner(f *fakeExec) *Runner {
	r := NewRunner(Settings{Stanza: "demo-catalog-pg18", Repo: Repo{Type: TypePosix}}, nil)
	r.Exec = f.run
	return r
}

func TestRunnerBackupReturnsNewestSet(t *testing.T) {
	info, _ := os.ReadFile("testdata/info.json")
	f := &fakeExec{output: map[string][]string{"backup": {"P00 INFO: backup command begin", "P00 INFO: backup command end"}, "info": {string(info)}}}
	r := NewRunner(Settings{Stanza: "t-catalog-pg18", Repo: Repo{Type: TypePosix}}, nil)
	r.Exec = f.run
	res, err := r.Backup(context.Background(), Incr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Info.Label != "20260819-030047F_20260819-030048I" {
		t.Errorf("label %q", res.Info.Label)
	}
	if len(res.Log) != 2 || res.Log[1] != "P00 INFO: backup command end" {
		t.Errorf("log %v", res.Log)
	}
	want := []string{"--config=/etc/pgbackrest/pgbackrest.conf", "--stanza=t-catalog-pg18", "--log-level-console=info", "--type=incr", "backup"}
	if strings.Join(f.calls[0], " ") != strings.Join(want, " ") {
		t.Errorf("args %v", f.calls[0])
	}
	if strings.Join(f.calls[1], " ") != "--config=/etc/pgbackrest/pgbackrest.conf --stanza=t-catalog-pg18 --log-level-console=info --output=json info" {
		t.Errorf("info args %v", f.calls[1])
	}
}

func TestRunnerBackupFailureKeepsLastLine(t *testing.T) {
	f := &fakeExec{fail: map[string]error{"backup": errors.New("exit status 41")}, output: map[string][]string{"backup": {"P00  ERROR: [041]: unable to connect", "P00   INFO: backup command end: aborted with exception [041]"}}}
	res, err := newRunner(f).Backup(context.Background(), Full)
	if err == nil || !strings.HasSuffix(err.Error(), "P00  ERROR: [041]: unable to connect") || !strings.Contains(err.Error(), "exit status 41") {
		t.Fatalf("err %v", err)
	}
	if len(res.Log) != 2 {
		t.Errorf("log %v", res.Log)
	}
	if len(f.calls) != 1 {
		t.Errorf("info must not run after a failed backup: %v", f.calls)
	}
}

func TestEnsureStanzaFallsBackToUpgrade(t *testing.T) {
	f := &fakeExec{}
	if err := newRunner(f).EnsureStanza(context.Background()); err != nil || len(f.calls) != 1 {
		t.Fatalf("create: %v %v", err, f.calls)
	}
	f = &fakeExec{fail: map[string]error{"stanza-create": errors.New("exit 28")}}
	if err := newRunner(f).EnsureStanza(context.Background()); err != nil || len(f.calls) != 2 || f.calls[1][len(f.calls[1])-1] != "stanza-upgrade" {
		t.Fatalf("upgrade: %v %v", err, f.calls)
	}
	f = &fakeExec{fail: map[string]error{"stanza-create": errors.New("exit 28"), "stanza-upgrade": errors.New("exit 29")}}
	if err := newRunner(f).EnsureStanza(context.Background()); err == nil || !strings.Contains(err.Error(), "exit 29") || !strings.Contains(err.Error(), "exit 28") {
		t.Fatalf("both: %v", err)
	}
}

func TestExpireVerifyAndLogTail(t *testing.T) {
	lines := make([]string, 0, 60)
	for i := 0; i < 60; i++ {
		lines = append(lines, "line")
	}
	f := &fakeExec{output: map[string][]string{"expire": lines, "verify": {"ok"}}}
	r := newRunner(f)
	tail, err := r.Expire(context.Background())
	if err != nil || len(tail) != logTail {
		t.Fatalf("expire %v %d", err, len(tail))
	}
	if tail, err := r.Verify(context.Background()); err != nil || len(tail) != 1 {
		t.Fatalf("verify %v %v", err, tail)
	}
}
