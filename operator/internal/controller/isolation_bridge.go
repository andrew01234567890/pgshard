//go:build !bridge

package controller

// isolationBuildAllowsActivation is the build-time isolation-activation ceiling.
// A default build permits activation. A `bridge`-tagged build sets it to false in
// isolation_bridge_disabled.go, and the activation state machine can then never
// leave INACTIVE regardless of any runtime input. This is deliberately a
// compile-time constant, not a runtime flag: a runtime flag may further disable
// activation, but nothing at runtime may enable it on a bridge binary.
const isolationBuildAllowsActivation = true
