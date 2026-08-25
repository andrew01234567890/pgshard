package backup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// Type is a pgbackrest backup type.
type Type string

// Backup types.
const (
	Full Type = "full"
	Diff Type = "diff"
	Incr Type = "incr"
)

// logTail is how many console lines a command result keeps.
const logTail = 50

// Runner executes pgbackrest commands for one stanza.
type Runner struct {
	Settings Settings
	Log      *slog.Logger
	// Env is appended to the process environment; the agent passes
	// PGPASSFILE so pgbackrest can reach the local server.
	Env []string
	// Exec runs the binary; swappable for tests. It returns combined output.
	Exec func(ctx context.Context, args []string, onLine func(string)) error
	// Start starts a command and returns a stop function to run after Wait,
	// letting the agent start and reaper-track the pgbackrest child
	// atomically so the PID1 reaper cannot reap it before Wait (ECHILD). A
	// nil Start starts the command directly.
	Start func(cmd *exec.Cmd) (stop func(), err error)
}

// NewRunner builds a Runner around the pgbackrest binary on PATH.
func NewRunner(s Settings, log *slog.Logger) *Runner {
	r := &Runner{Settings: s.WithDefaults(), Log: log}
	r.Exec = r.execBinary
	return r
}

func (r *Runner) args(cmd string, extra ...string) []string {
	base := []string{"--config=" + r.Settings.ConfigPath, "--stanza=" + r.Settings.Stanza, "--log-level-console=" + r.Settings.LogLevel}
	return append(append(base, extra...), cmd)
}

func (r *Runner) execBinary(ctx context.Context, args []string, onLine func(string)) error {
	cmd := exec.CommandContext(ctx, "pgbackrest", args...)
	cmd.Env = append(os.Environ(), r.Env...)
	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw
	start := r.Start
	if start == nil {
		start = func(c *exec.Cmd) (func(), error) { return func() {}, c.Start() }
	}
	stop, err := start(cmd)
	if err != nil {
		_ = pw.Close()
		return err
	}
	defer stop()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			onLine(sc.Text())
		}
	}()
	err = cmd.Wait()
	_ = pw.Close()
	wg.Wait()
	return err
}

// run executes a pgbackrest command, logs its output and returns the tail.
func (r *Runner) run(ctx context.Context, cmd string, extra ...string) ([]string, error) {
	var mu sync.Mutex
	var tail []string
	onLine := func(l string) {
		if r.Log != nil {
			r.Log.Info("pgbackrest", "cmd", cmd, "line", l)
		}
		mu.Lock()
		tail = append(tail, l)
		if len(tail) > logTail {
			tail = tail[len(tail)-logTail:]
		}
		mu.Unlock()
	}
	err := r.Exec(ctx, r.args(cmd, extra...), onLine)
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		return tail, fmt.Errorf("pgbackrest %s: %w: %s", cmd, err, lastLine(tail))
	}
	return tail, nil
}

// lastLine picks the most useful line of the tail: the last ERROR line when
// there is one, else the last non-empty line.
func lastLine(tail []string) string {
	last := ""
	for i := len(tail) - 1; i >= 0; i-- {
		if strings.Contains(tail[i], " ERROR: ") {
			return strings.TrimSpace(tail[i])
		}
		if last == "" && strings.TrimSpace(tail[i]) != "" {
			last = strings.TrimSpace(tail[i])
		}
	}
	return last
}

// lockExitCode is pgbackrest's LockAcquireError (050): another command,
// typically the asynchronous archive-push started by archive_command,
// holds the stanza lock.
const lockExitCode = 50

// LockBusy reports whether err is a pgbackrest command that lost the race
// for the stanza lock, a transient condition worth an immediate retry.
func LockBusy(err error) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == lockExitCode
}

// EnsureStanza creates the stanza, upgrading it when the repository already
// holds one for a previous PostgreSQL system identifier.
func (r *Runner) EnsureStanza(ctx context.Context) error {
	if _, err := r.run(ctx, "stanza-create"); err == nil {
		return nil
	} else if _, uerr := r.run(ctx, "stanza-upgrade"); uerr != nil {
		return errors.Join(err, uerr)
	}
	return nil
}

// Result is what a completed backup produced.
type Result struct {
	Info Info
	Log  []string
}

// Backup takes a backup of the given type and returns the resulting set,
// found as the newest backup in the repository.
func (r *Runner) Backup(ctx context.Context, t Type) (Result, error) {
	tail, err := r.run(ctx, "backup", "--type="+string(t))
	if err != nil {
		return Result{Log: tail}, err
	}
	st, err := r.Info(ctx)
	if err != nil {
		return Result{Log: tail}, err
	}
	if len(st.Backups) == 0 {
		return Result{Log: tail}, errors.New("backup finished but the repository lists no backups")
	}
	return Result{Info: st.Backups[len(st.Backups)-1], Log: tail}, nil
}

// Expire applies retention.
func (r *Runner) Expire(ctx context.Context) ([]string, error) { return r.run(ctx, "expire") }

// Verify checks the repository.
func (r *Runner) Verify(ctx context.Context) ([]string, error) { return r.run(ctx, "verify") }

// Stanza is the parsed `pgbackrest info --output=json` for one stanza.
type Stanza struct {
	Name          string
	StatusCode    int64
	StatusMessage string
	ArchiveMin    string
	ArchiveMax    string
	Backups       []Info
}

// Info describes one backup set.
type Info struct {
	Label        string
	Type         string
	Prior        string
	StartLSN     uint64
	StopLSN      uint64
	ArchiveStart string
	ArchiveStop  string
	SizeBytes    uint64
	// RepoBytes is what the set added to the repository (bundled repositories
	// report only the delta).
	RepoBytes  uint64
	StartedAt  int64
	FinishedAt int64
}

// Info runs `info --output=json` for the stanza.
func (r *Runner) Info(ctx context.Context) (Stanza, error) {
	var buf bytes.Buffer
	err := r.Exec(ctx, r.args("info", "--output=json"), func(l string) { buf.WriteString(l); buf.WriteByte('\n') })
	if err != nil {
		return Stanza{}, fmt.Errorf("pgbackrest info: %w: %s", err, strings.TrimSpace(buf.String()))
	}
	return ParseInfo(buf.Bytes(), r.Settings.Stanza)
}

type infoJSON struct {
	Name   string `json:"name"`
	Status struct {
		Code    int64  `json:"code"`
		Message string `json:"message"`
	} `json:"status"`
	Archive []struct {
		Min string `json:"min"`
		Max string `json:"max"`
	} `json:"archive"`
	Backup []struct {
		Label   string `json:"label"`
		Type    string `json:"type"`
		Prior   string `json:"prior"`
		Archive struct {
			Start string `json:"start"`
			Stop  string `json:"stop"`
		} `json:"archive"`
		LSN struct {
			Start string `json:"start"`
			Stop  string `json:"stop"`
		} `json:"lsn"`
		Timestamp struct {
			Start int64 `json:"start"`
			Stop  int64 `json:"stop"`
		} `json:"timestamp"`
		Info struct {
			Size       uint64 `json:"size"`
			Repository struct {
				Size  uint64 `json:"size"`
				Delta uint64 `json:"delta"`
			} `json:"repository"`
		} `json:"info"`
	} `json:"backup"`
}

// ParseInfo decodes pgbackrest info JSON and picks the named stanza.
func ParseInfo(b []byte, stanza string) (Stanza, error) {
	var all []infoJSON
	if err := json.Unmarshal(b, &all); err != nil {
		return Stanza{}, fmt.Errorf("parse pgbackrest info: %w", err)
	}
	for _, s := range all {
		if s.Name != stanza {
			continue
		}
		out := Stanza{Name: s.Name, StatusCode: s.Status.Code, StatusMessage: s.Status.Message}
		if len(s.Archive) > 0 {
			last := s.Archive[len(s.Archive)-1]
			out.ArchiveMin, out.ArchiveMax = last.Min, last.Max
		}
		for _, bk := range s.Backup {
			start, err := ParseLSN(bk.LSN.Start)
			if err != nil {
				return Stanza{}, fmt.Errorf("backup %s: %w", bk.Label, err)
			}
			stop, err := ParseLSN(bk.LSN.Stop)
			if err != nil {
				return Stanza{}, fmt.Errorf("backup %s: %w", bk.Label, err)
			}
			out.Backups = append(out.Backups, Info{
				Label: bk.Label, Type: bk.Type, Prior: bk.Prior,
				StartLSN: start, StopLSN: stop,
				ArchiveStart: bk.Archive.Start, ArchiveStop: bk.Archive.Stop,
				SizeBytes: bk.Info.Size, RepoBytes: max(bk.Info.Repository.Size, bk.Info.Repository.Delta),
				StartedAt: bk.Timestamp.Start, FinishedAt: bk.Timestamp.Stop,
			})
		}
		return out, nil
	}
	return Stanza{}, fmt.Errorf("stanza %q not in pgbackrest info", stanza)
}

// ParseLSN converts X/Y notation to a byte offset; empty is 0.
func ParseLSN(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	var hi, lo uint64
	if _, err := fmt.Sscanf(s, "%x/%x", &hi, &lo); err != nil {
		return 0, fmt.Errorf("lsn %q: %w", s, err)
	}
	return hi<<32 | lo, nil
}
