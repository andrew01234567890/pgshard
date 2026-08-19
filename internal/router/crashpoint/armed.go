//go:build pgshard_crashpoints

package crashpoint

import (
	"os"
	"syscall"
)

// EnvVar names the crash point to arm.
const EnvVar = "PGSHARD_TEST_CRASH_POINT"

func hit(name string) {
	if os.Getenv(EnvVar) != name {
		return
	}
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}
