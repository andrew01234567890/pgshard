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
		{group: "coordination.k8s.io", resource: "leases", verbs: []string{"create", "get", "list", "patch", "update", "watch"}},
		{group: "", resource: "events", verbs: []string{"create", "patch"}},
		{group: "", resource: "persistentvolumeclaims", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
	} {
		if !roleAllows(role, required.group, required.resource, required.verbs) {
			t.Errorf("manager role does not authorize %q %q with verbs %v", required.group, required.resource, required.verbs)
		}
	}
}

func roleAllows(role *rbacv1.ClusterRole, group, resource string, verbs []string) bool {
	for _, rule := range role.Rules {
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
