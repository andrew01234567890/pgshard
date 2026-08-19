package admin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestAdminRBACManifestIsReadOnly(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "admin", "rbac.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	readOnly := map[string]bool{"get": true, "list": true, "watch": true}
	roles := 0
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatal(err)
		}
		if meta.Kind != "ClusterRole" && meta.Kind != "Role" {
			continue
		}
		roles++
		var role rbacv1.ClusterRole
		if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
			t.Fatal(err)
		}
		if len(role.Rules) == 0 {
			t.Fatalf("%s %s has no rules", meta.Kind, role.Name)
		}
		for _, rule := range role.Rules {
			if len(rule.Verbs) == 0 {
				t.Errorf("rule %v has no verbs", rule.Resources)
			}
			for _, v := range rule.Verbs {
				if !readOnly[v] {
					t.Errorf("rule for %v grants non-read verb %q", rule.Resources, v)
				}
			}
			for _, r := range rule.Resources {
				if r == "*" || strings.HasSuffix(r, "/status") {
					t.Errorf("rule grants %q", r)
				}
			}
		}
	}
	if roles == 0 {
		t.Fatal("no roles found in rbac.yaml")
	}
}
