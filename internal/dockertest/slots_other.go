//go:build !unix

package dockertest

// acquireSlot is a no-op where there is no flock. The in-process bound
// still applies; only the bound ACROSS test binaries is missing, which
// leaves such a platform exactly where every platform was before.
func acquireSlot(string, int) (func(), error) { return func() {}, nil }
