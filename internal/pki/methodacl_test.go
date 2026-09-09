package pki

import "testing"

// The callers table decides who may CONNECT to a listener; the methods
// table decides what connecting buys them. Before the second, the
// controller admitted {router, operator} and every admitted role reached
// every method it serves -- so a router's certificate was also a
// credential for CancelWorkflow, PauseWorkflow, ResolveTransactions and
// CreateBarrier. This is the narrowing (PGS-503).
func TestARouterMayOnlyCallTheControllerMethodsItActuallyUses(t *testing.T) {
	router := Identity{Namespace: "ns", Cluster: "demo", Role: RoleRouter}

	// The two the router's ControllerClient is built to call, and the only
	// two it calls anywhere in the tree.
	for _, m := range []string{
		"/pgshard.v1.Controller/CreateStream",
		"/pgshard.v1.Controller/DropStream",
	} {
		if !AllowedMethod(RoleController, router, m) {
			t.Errorf("a router must be able to call %s", m)
		}
	}

	// Everything else the controller serves. A leaked or borrowed router
	// certificate reaching any of these is the escalation this exists to
	// close.
	for _, m := range []string{
		"/pgshard.v1.Controller/CancelWorkflow",
		"/pgshard.v1.Controller/PauseWorkflow",
		"/pgshard.v1.Controller/ResumeWorkflow",
		"/pgshard.v1.Controller/ResolveTransactions",
		"/pgshard.v1.Controller/CreateBarrier",
		"/pgshard.v1.Controller/ListBarriers",
		"/pgshard.v1.Controller/ListRoleStatus",
	} {
		if AllowedMethod(RoleController, router, m) {
			t.Errorf("a router must not be able to call %s", m)
		}
	}
}

// The operator is deliberately unrestricted on the controller: its
// certificate is also what a human administrator presents today, since
// RoleAdmin exists but appears in no caller list. Narrowing it would
// decide who may administer the cluster, which is a policy question rather
// than a fact about the code.
func TestTheOperatorIsNotNarrowedOnTheController(t *testing.T) {
	op := Identity{Namespace: "ns", Cluster: "demo", Role: RoleOperator}
	for _, m := range []string{
		"/pgshard.v1.Controller/CreateBarrier",
		"/pgshard.v1.Controller/CancelWorkflow",
		"/pgshard.v1.Controller/ResolveTransactions",
	} {
		if !AllowedMethod(RoleController, op, m) {
			t.Errorf("the operator must still be able to call %s", m)
		}
	}
}

// A listener with no rule authorises every method, the same way a listener
// with no caller rule accepts whatever chains to the CA. Asserted so that
// adding a rule for one listener cannot silently start refusing calls on
// another.
func TestAListenerWithNoMethodRuleIsUnrestricted(t *testing.T) {
	router := Identity{Namespace: "ns", Cluster: "demo", Role: RoleRouter}
	if !AllowedMethod(RolePooler, router, "/pgshard.v1.Pooler/Execute") {
		t.Error("the pooler has no method rule, so it must admit every method")
	}
	if !AllowedMethod(RoleAgent, router, "/pgshard.v1.Agent/Promote") {
		t.Error("the agent has no method rule, so it must admit every method")
	}
}

// A caller role added to a listener's callers list gets EVERY method until
// someone remembers this table, because a pair with no rule is
// unrestricted. Nothing couples the two tables, so this is what couples
// them: adding a caller becomes a decision about what it may call, taken
// deliberately here rather than by default elsewhere.
//
// Exempt roles are listed rather than inferred. The operator is exempt
// because its certificate is also a human administrator's today (RoleAdmin
// exists but is in no caller list), so narrowing it decides who may
// administer a cluster -- a policy question, not a fact about the code.
func TestEveryCallerOfARestrictedListenerHasAMethodRule(t *testing.T) {
	exempt := map[string]map[string]bool{
		RoleController: {RoleOperator: true},
	}
	for listener, byRole := range methods {
		allowed, ok := callers[listener]
		if !ok {
			t.Errorf("listener %q has method rules but no caller rule, so nothing reaches them", listener)
			continue
		}
		for _, caller := range allowed {
			if exempt[listener][caller] || len(byRole[caller]) > 0 {
				continue
			}
			t.Errorf("listener %q admits %q with no method rule and no exemption: it may call everything", listener, caller)
		}
	}
}
