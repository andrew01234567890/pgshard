package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ShutdownMode maps to the postmaster's shutdown signals.
type ShutdownMode int

// Shutdown modes in order of severity.
const (
	ShutdownSmart ShutdownMode = iota
	ShutdownFast
	ShutdownImmediate
)

func (m ShutdownMode) signal() syscall.Signal {
	switch m {
	case ShutdownFast:
		return syscall.SIGINT
	case ShutdownImmediate:
		return syscall.SIGQUIT
	default:
		return syscall.SIGTERM
	}
}

// Supervisor owns the postgres child process. It is safe for concurrent use.
type Supervisor struct {
	binDir string
	pgdata string
	log    *slog.Logger
	// OnUnexpectedExit is called when postgres exits without Stop being asked.
	OnUnexpectedExit func(error)
	// Env is appended to the environment of postgres and every tool.
	Env []string

	mu       sync.Mutex
	stopping bool
	cmd      *exec.Cmd
	done     chan struct{}
	tracked  map[int]struct{}
}

// NewSupervisor creates a supervisor for the instance in pgdata.
func NewSupervisor(binDir, pgdata string, log *slog.Logger) *Supervisor {
	return &Supervisor{binDir: binDir, pgdata: pgdata, log: log, tracked: map[int]struct{}{}}
}

// Start launches postgres. It fails if a child is already running.
func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil {
		return errors.New("postgres already running")
	}
	cmd := exec.Command(filepath.Join(s.binDir, "postgres"), "-D", s.pgdata)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), s.Env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	s.cmd = cmd
	s.stopping = false
	s.done = make(chan struct{})
	s.tracked[cmd.Process.Pid] = struct{}{}
	done := s.done
	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		delete(s.tracked, cmd.Process.Pid)
		s.cmd = nil
		unexpected := !s.stopping
		s.mu.Unlock()
		s.log.Info("postgres exited", "err", err, "expected", !unexpected)
		close(done)
		if unexpected && s.OnUnexpectedExit != nil {
			s.OnUnexpectedExit(fmt.Errorf("postgres exited unexpectedly: %w", err))
		}
	}()
	s.log.Info("postgres started", "pid", cmd.Process.Pid)
	return nil
}

// Running reports whether the child is alive.
func (s *Supervisor) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cmd != nil
}

// Done returns a channel closed when the current child exits; nil when no
// child is running.
func (s *Supervisor) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// Stop signals the postmaster with mode and waits up to timeout; on timeout
// it escalates to the next mode until the child is gone.
func (s *Supervisor) Stop(ctx context.Context, mode ShutdownMode, timeout time.Duration) error {
	for {
		s.mu.Lock()
		cmd, done := s.cmd, s.done
		s.stopping = true
		s.mu.Unlock()
		if cmd == nil {
			return nil
		}
		s.log.Info("stopping postgres", "mode", mode, "timeout", timeout)
		if err := cmd.Process.Signal(mode.signal()); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(timeout):
			if mode == ShutdownImmediate {
				return errors.New("postgres did not exit after SIGQUIT")
			}
			mode++
		}
	}
}

// Track registers a pid the supervisor owns so the reaper leaves it alone.
func (s *Supervisor) Track(pid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracked[pid] = struct{}{}
}

// Untrack releases a pid registered with Track.
func (s *Supervisor) Untrack(pid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tracked, pid)
}

// Command builds an exec.Cmd for a binary in binDir with the given
// environment, tracked so the reaper does not steal its exit status.
func (s *Supervisor) Command(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, filepath.Join(s.binDir, name), args...)
	cmd.Env = append(append(os.Environ(), "PGDATA="+s.pgdata), s.Env...)
	return cmd
}

// RunTracked runs cmd to completion while keeping its pid out of the reaper.
func (s *Supervisor) RunTracked(cmd *exec.Cmd) ([]byte, error) {
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	// Start and track under the lock so the reaper, which snapshots the
	// tracked set under the same lock, can never observe this child as an
	// untracked orphan between Start and Track and reap it out from under
	// cmd.Wait() (which would then fail with ECHILD).
	s.mu.Lock()
	if err := cmd.Start(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.tracked[cmd.Process.Pid] = struct{}{}
	s.mu.Unlock()
	err := cmd.Wait()
	s.Untrack(cmd.Process.Pid)
	if err != nil {
		return []byte(out.String()), fmt.Errorf("%s: %w: %s", filepath.Base(cmd.Path), err, strings.TrimSpace(out.String()))
	}
	return []byte(out.String()), nil
}

// ReapOrphans runs until ctx is done, reaping zombie children that are not
// tracked. Only meaningful when the agent is PID 1.
func (s *Supervisor) ReapOrphans(ctx context.Context) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGCHLD)
	defer signal.Stop(sig)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		for _, pid := range s.zombieOrphans() {
			var ws syscall.WaitStatus
			if _, err := syscall.Wait4(pid, &ws, syscall.WNOHANG, nil); err == nil {
				s.log.Info("reaped orphan", "pid", pid, "status", ws.ExitStatus())
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-sig:
		case <-tick.C:
		}
	}
}

// zombieOrphans lists zombie processes whose parent is this process and that
// no exec.Cmd is waiting on.
func (s *Supervisor) zombieOrphans() []int {
	self := os.Getpid()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	s.mu.Lock()
	tracked := make(map[int]struct{}, len(s.tracked))
	for pid := range s.tracked {
		tracked[pid] = struct{}{}
	}
	s.mu.Unlock()
	var out []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		if _, ok := tracked[pid]; ok {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue
		}
		// pid (comm) state ppid ... comm may contain spaces; split after ')'.
		i := strings.LastIndexByte(string(b), ')')
		if i < 0 {
			continue
		}
		fields := strings.Fields(string(b[i+1:]))
		if len(fields) < 2 || fields[0] != "Z" {
			continue
		}
		if ppid, _ := strconv.Atoi(fields[1]); ppid == self {
			out = append(out, pid)
		}
	}
	return out
}
