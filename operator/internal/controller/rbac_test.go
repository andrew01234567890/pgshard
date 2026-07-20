package controller

import (
	"os"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func TestGeneratedManagerRoleAuthorizesRuntimeControlPaths(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../config/rbac/role.yaml")
	if err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.Unmarshal(contents, role); err != nil {
		t.Fatal(err)
	}
	for _, required := range []struct {
		group    string
		resource string
		verbs    []string
	}{
		{group: "", resource: "events", verbs: []string{"create", "patch"}},
		{group: "", resource: "namespaces", verbs: []string{"get"}},
		{group: "", resource: "nodes", verbs: []string{"get"}},
		{group: "", resource: "persistentvolumeclaims", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "", resource: "pods", verbs: []string{"delete", "get", "list", "patch"}},
		{group: "", resource: "secrets", verbs: []string{"create", "delete", "get", "update"}},
		{group: "", resource: "serviceaccounts", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "coordination.k8s.io", resource: "leases", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "rbac.authorization.k8s.io", resource: "roles", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "rbac.authorization.k8s.io", resource: "rolebindings", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "storage.k8s.io", resource: "storageclasses", verbs: []string{"list"}},
	} {
		if !roleAllows(role, required.group, required.resource, required.verbs) {
			t.Errorf("manager role does not authorize %q %q with verbs %v", required.group, required.resource, required.verbs)
		}
	}
	for _, forbidden := range []string{"list", "patch", "watch"} {
		if roleAllows(role, "", "secrets", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden Secret verb %q", forbidden)
		}
	}
	for _, forbidden := range []string{"create", "delete", "get", "patch", "update", "watch"} {
		if roleAllows(role, "storage.k8s.io", "storageclasses", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden StorageClass verb %q", forbidden)
		}
	}
	for _, forbidden := range []string{"create", "delete", "list", "patch", "update", "watch"} {
		if roleAllows(role, "", "namespaces", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden Namespace verb %q", forbidden)
		}
	}
	for _, forbidden := range []string{"create", "delete", "get", "list", "patch", "update", "watch"} {
		if roleAllows(role, "", "endpoints", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden Endpoints verb %q", forbidden)
		}
	}
	for _, forbidden := range []string{"create", "update", "watch"} {
		if roleAllows(role, "", "pods", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden Pod verb %q", forbidden)
		}
	}
	for _, forbidden := range []string{"create", "delete", "list", "patch", "update", "watch"} {
		if roleAllows(role, "", "nodes", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden Node verb %q", forbidden)
		}
	}
}

func TestLeaderElectionRoleIsNamespaced(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../config/rbac/leader_election_role.yaml")
	if err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.Role{}
	if err := yaml.UnmarshalStrict(contents, role); err != nil {
		t.Fatal(err)
	}
	if role.Namespace != "system" {
		t.Fatalf("leader-election Role namespace = %q", role.Namespace)
	}
	if !rulesAllow(role.Rules, "coordination.k8s.io", "leases", []string{"create", "delete", "get", "list", "patch", "update", "watch"}) {
		t.Fatalf("leader-election Role rules = %#v", role.Rules)
	}
}

func roleAllows(role *rbacv1.ClusterRole, group, resource string, verbs []string) bool {
	return rulesAllow(role.Rules, group, resource, verbs)
}

func rulesAllow(rules []rbacv1.PolicyRule, group, resource string, verbs []string) bool {
	allowed := make(map[string]struct{})
	for _, rule := range rules {
		if (!slices.Contains(rule.APIGroups, group) && !slices.Contains(rule.APIGroups, "*")) ||
			(!slices.Contains(rule.Resources, resource) && !slices.Contains(rule.Resources, "*")) {
			continue
		}
		for _, verb := range rule.Verbs {
			allowed[verb] = struct{}{}
		}
	}
	if _, wildcard := allowed["*"]; wildcard {
		return true
	}
	for _, verb := range verbs {
		if _, ok := allowed[verb]; !ok {
			return false
		}
	}
	return len(verbs) > 0
}

func TestRulesAllowCombinesMatchingRules(t *testing.T) {
	t.Parallel()
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"list"}},
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"delete"}},
	}
	if !rulesAllow(rules, "", "pods", []string{"list", "delete"}) {
		t.Fatal("matching RBAC rules were not combined")
	}
	if rulesAllow(rules, "", "pods", []string{"get"}) {
		t.Fatal("ungranted verb was inferred from matching RBAC rules")
	}
}
