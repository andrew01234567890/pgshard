package controller

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
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
		{group: "", resource: "endpoints", verbs: []string{"get"}},
		{group: "", resource: "events", verbs: []string{"create", "patch"}},
		{group: "", resource: "namespaces", verbs: []string{"get"}},
		{group: "", resource: "nodes", verbs: []string{"get"}},
		{group: "", resource: "persistentvolumeclaims", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "", resource: "pods", verbs: []string{"delete", "get", "list", "patch"}},
		{group: "", resource: "secrets", verbs: []string{"create", "delete", "get", "update"}},
		{group: "", resource: "serviceaccounts", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "coordination.k8s.io", resource: "leases", verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{group: "pgshard.io", resource: "pgshardcatalogactivations", verbs: []string{"create", "get", "list", "watch"}},
		{group: "pgshard.io", resource: "pgshardcatalogactivations/status", verbs: []string{"update"}},
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
	for _, forbidden := range []string{"create", "delete", "list", "patch", "update", "watch"} {
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
	for _, forbidden := range []string{"delete", "patch", "update"} {
		if roleAllows(role, "pgshard.io", "pgshardcatalogactivations", []string{forbidden}) {
			t.Errorf("cluster-wide manager role grants forbidden catalog activation verb %q", forbidden)
		}
	}
	for _, verb := range []string{"create", "delete", "get", "list", "patch", "watch"} {
		if roleAllows(role, "pgshard.io", "pgshardcatalogactivations/status", []string{verb}) {
			t.Errorf("cluster-wide manager role grants forbidden catalog activation status verb %q", verb)
		}
	}
	for _, verb := range []string{"create", "delete", "get", "list", "patch", "update", "watch"} {
		if roleAllows(role, "pgshard.io", "pgshardcatalogactivations/finalizers", []string{verb}) {
			t.Errorf("cluster-wide manager role grants forbidden catalog activation finalizers verb %q", verb)
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

// The manager service account, as authored in the RBAC manifests (before the
// kustomize namePrefix is applied).
const (
	managerSubjectName      = "controller-manager"
	managerSubjectNamespace = "system"
)

type projectRole struct {
	kind        string
	name        string
	rules       []rbacv1.PolicyRule
	aggregation *rbacv1.AggregationRule
}

type projectBinding struct {
	roleRef  rbacv1.RoleRef
	subjects []rbacv1.Subject
}

func ruleCovers(values []string, want string) bool {
	return slices.Contains(values, want) || slices.Contains(values, "*")
}

func policyRuleGrants(rule rbacv1.PolicyRule, apiGroup, verb string, resources ...string) bool {
	if !ruleCovers(rule.APIGroups, apiGroup) || !ruleCovers(rule.Verbs, verb) {
		return false
	}
	for _, resource := range rule.Resources {
		if resource == "*" || slices.Contains(resources, resource) {
			return true
		}
	}
	return false
}

func grantsManagerTokenCreate(rule rbacv1.PolicyRule) bool {
	return policyRuleGrants(rule, "", "create", "serviceaccounts/token")
}

func grantsImpersonation(rule rbacv1.PolicyRule) bool {
	return policyRuleGrants(rule, "", "impersonate", "users", "groups", "serviceaccounts")
}

func roleRefIsProjectLocal(ref rbacv1.RoleRef, known map[string]struct{}) bool {
	if ref.APIGroup != "rbac.authorization.k8s.io" || (ref.Kind != "Role" && ref.Kind != "ClusterRole") {
		return false
	}
	_, ok := known[ref.Kind+"/"+ref.Name]
	return ok
}

func bindsManager(subjects []rbacv1.Subject) bool {
	for _, subject := range subjects {
		if subject.Kind == "ServiceAccount" && subject.Name == managerSubjectName && subject.Namespace == managerSubjectNamespace {
			return true
		}
	}
	return false
}

// discoverProjectRBAC renders every RBAC object reachable from both RBAC
// kustomizations by recursively following their `resources` lists, so a Role,
// ClusterRole, or binding added to either kustomization is automatically covered
// without editing this test.
func discoverProjectRBAC(t *testing.T) ([]projectRole, []projectBinding) {
	t.Helper()
	var roles []projectRole
	var bindings []projectBinding
	seen := map[string]bool{}
	var walk func(dir string)
	walk = func(dir string) {
		data, err := os.ReadFile(filepath.Join(dir, "kustomization.yaml"))
		if err != nil {
			t.Fatalf("read kustomization in %s: %v", dir, err)
		}
		var kustomization struct {
			Resources []string `json:"resources"`
		}
		if err := yaml.Unmarshal(data, &kustomization); err != nil {
			t.Fatalf("decode kustomization in %s: %v", dir, err)
		}
		for _, resource := range kustomization.Resources {
			path := filepath.Clean(filepath.Join(dir, resource))
			if seen[path] {
				continue
			}
			seen[path] = true
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat RBAC resource %s: %v", path, err)
			}
			if info.IsDir() {
				walk(path)
				continue
			}
			decodeRBACDocuments(t, path, &roles, &bindings)
		}
	}
	walk("../../config/rbac")
	walk("../../config/admission/rbac")
	return roles, bindings
}

func decodeRBACDocuments(t *testing.T, path string, roles *[]projectRole, bindings *[]projectBinding) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RBAC manifest %s: %v", path, err)
	}
	reader := yamlutil.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	for {
		document, err := reader.Read()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatalf("read document in %s: %v", path, err)
		}
		if len(bytes.TrimSpace(document)) == 0 {
			continue
		}
		var meta metav1.TypeMeta
		if err := yaml.Unmarshal(document, &meta); err != nil {
			t.Fatalf("decode kind in %s: %v", path, err)
		}
		switch meta.Kind {
		case "ClusterRole":
			var role rbacv1.ClusterRole
			mustDecode(t, path, document, &role)
			*roles = append(*roles, projectRole{"ClusterRole", role.Name, role.Rules, role.AggregationRule})
		case "Role":
			var role rbacv1.Role
			mustDecode(t, path, document, &role)
			*roles = append(*roles, projectRole{"Role", role.Name, role.Rules, nil})
		case "ClusterRoleBinding":
			var binding rbacv1.ClusterRoleBinding
			mustDecode(t, path, document, &binding)
			*bindings = append(*bindings, projectBinding{binding.RoleRef, binding.Subjects})
		case "RoleBinding":
			var binding rbacv1.RoleBinding
			mustDecode(t, path, document, &binding)
			*bindings = append(*bindings, projectBinding{binding.RoleRef, binding.Subjects})
		default:
			t.Fatalf("unexpected object kind %q in RBAC manifest %s", meta.Kind, path)
		}
	}
}

func mustDecode(t *testing.T, path string, document []byte, into any) {
	t.Helper()
	if err := yaml.UnmarshalStrict(document, into); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func TestManagerRBACGrantsNoTokenOrImpersonationEscalation(t *testing.T) {
	t.Parallel()
	roles, bindings := discoverProjectRBAC(t)

	known := map[string]struct{}{}
	byKindName := map[string]projectRole{}
	for _, role := range roles {
		known[role.kind+"/"+role.name] = struct{}{}
		byKindName[role.kind+"/"+role.name] = role
		for _, rule := range role.rules {
			if grantsManagerTokenCreate(rule) {
				t.Errorf("%s %q grants serviceaccounts/token create", role.kind, role.name)
			}
			if grantsImpersonation(rule) {
				t.Errorf("%s %q grants impersonation of users/groups/serviceaccounts", role.kind, role.name)
			}
		}
	}

	managerBound := 0
	for _, binding := range bindings {
		if !bindsManager(binding.subjects) {
			continue
		}
		managerBound++
		if !roleRefIsProjectLocal(binding.roleRef, known) {
			t.Errorf("manager-bound binding references a non-project-local role: %#v", binding.roleRef)
			continue
		}
		if role, ok := byKindName[binding.roleRef.Kind+"/"+binding.roleRef.Name]; ok && role.aggregation != nil {
			t.Errorf("manager-bound %s %q uses an aggregationRule", role.kind, role.name)
		}
	}
	if managerBound == 0 {
		t.Fatal("no manager-bound role bindings were found; the escalation assertions never ran")
	}
}

func TestManagerRBACAssertionsCatchPlantedEscalation(t *testing.T) {
	t.Parallel()
	tokenRule := rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"}, Verbs: []string{"create"}}
	if !grantsManagerTokenCreate(tokenRule) {
		t.Fatal("a planted serviceaccounts/token create grant was not detected")
	}
	impersonateRule := rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"users"}, Verbs: []string{"impersonate"}}
	if !grantsImpersonation(impersonateRule) {
		t.Fatal("a planted impersonation grant was not detected")
	}
	wildcard := rbacv1.PolicyRule{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"*"}}
	if !grantsManagerTokenCreate(wildcard) || !grantsImpersonation(wildcard) {
		t.Fatal("a wildcard escalation was not detected")
	}
	benign := rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"serviceaccounts", "pods"}, Verbs: []string{"create", "get", "list"}}
	if grantsManagerTokenCreate(benign) || grantsImpersonation(benign) {
		t.Fatal("a benign serviceaccounts/pods rule was wrongly flagged")
	}

	known := map[string]struct{}{"ClusterRole/manager-role": {}}
	if roleRefIsProjectLocal(rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster-admin"}, known) {
		t.Fatal("a manager binding to the built-in cluster-admin was accepted")
	}
	if !roleRefIsProjectLocal(rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "manager-role"}, known) {
		t.Fatal("the project-local manager-role roleRef was rejected")
	}
}
