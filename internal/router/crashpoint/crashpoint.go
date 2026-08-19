// Package crashpoint lets test builds kill the router at named points of the
// commit protocol. Without the pgshard_crashpoints build tag Hit is a no-op.
package crashpoint

// Hit kills the process when the named point is armed.
func Hit(name string) { hit(name) }
