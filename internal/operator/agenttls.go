package operator

import "sync"

// AgentTLSModes records, per agent address, whether that member's agent was
// started requiring mutual TLS.
//
// It exists because a rollout makes the fleet mixed: members restart one at a
// time, so an agent that has restarted requires TLS while its neighbour still
// serves plaintext. A caller with one answer for the whole fleet is wrong
// about half of it, in whichever direction it guesses.
//
// The operator fills this from the pod annotation each pass, before it calls
// any agent. It cannot ask the agents instead: one that requires TLS will not
// answer a plaintext caller, so a protocol that discovers the mode by calling
// has to solve this problem before it can run.
type AgentTLSModes struct {
	mu sync.RWMutex
	m  map[string]bool
}

// Set records how the agent at addr must be dialled.
func (a *AgentTLSModes) Set(addr string, requiresTLS bool) {
	if a == nil || addr == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.m == nil {
		a.m = map[string]bool{}
	}
	a.m[addr] = requiresTLS
}

// Requires reports whether the agent at addr requires mutual TLS. An address
// nothing has recorded is plaintext: a member the operator has not observed
// this pass is one it has not yet started with the requirement.
func (a *AgentTLSModes) Requires(addr string) bool {
	if a == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.m[addr]
}
