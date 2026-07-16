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
		{group: "", resource: "persistentvolumeclaims", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "", resource: "secrets", verbs: []string{"create", "get"}},
		{group: "storage.k8s.io", resource: "storageclasses", verbs: []string{"get"}},
	} {
		if !roleAllows(role, required.group, required.resource, required.verbs) {
			t.Errorf("manager role does not authorize %q %q with verbs %v", required.group, required.resource, required.verbs)
		}
	}
	if roleAllows(role, "coordination.k8s.io", "leases", []string{"get"}) {
		t.Fatal("cluster-wide manager role must not grant leader-election Lease access")
	}
	for _, forbidden := range []string{"delete", "list", "patch", "update", "watch"} {
		if roleAllows(role, "", "secrets", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden Secret verb %q", forbidden)
		}
	}
	for _, forbidden := range []string{"create", "delete", "list", "patch", "update", "watch"} {
		if roleAllows(role, "storage.k8s.io", "storageclasses", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden StorageClass verb %q", forbidden)
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
	for _, rule := range rules {
		if !slices.Contains(rule.APIGroups, group) || !slices.Contains(rule.Resources, resource) {
			continue
		}
		for _, verb := range verbs {
			if !slices.Contains(rule.Verbs, verb) && !slices.Contains(rule.Verbs, "*") {
				return false
			}
		}
		return true
	}
	return false
}
