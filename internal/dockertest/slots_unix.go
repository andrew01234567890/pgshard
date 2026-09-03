//go:build unix

package dockertest

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// acquireSlot takes one of n cross-process slots, blocking until one is
// free, and returns the release.
//
// The lock is flock on a file per slot. That choice matters for one
// reason: the kernel releases an flock when the holding process dies, so a
// test binary killed mid-run leaves nothing behind. A lock file holding a
// PID, or a counter in a file, would need a liveness check and a repair
// path, and would eventually wedge a developer's machine in a way only
// deleting a file fixes.
func acquireSlot(dir string, n int) (func(), error) {
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, err
	}
	for {
		for i := range n {
			f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("slot-%d", i)), os.O_CREATE|os.O_RDWR, 0o666)
			if err != nil {
				return nil, err
			}
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
				return func() { _ = f.Close() }, nil
			}
			_ = f.Close()
		}
		// Every slot is held by somebody. Waiting is the point: the whole
		// bound exists because the daemon cannot start containers as fast
		// as the suites ask it to.
		time.Sleep(25 * time.Millisecond)
	}
}
