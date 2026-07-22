//go:build bridge

package controller

// isolationBuildAllowsActivation is false on a bridge build: isolation activation
// is impossible and the state machine can never leave INACTIVE.
const isolationBuildAllowsActivation = false
