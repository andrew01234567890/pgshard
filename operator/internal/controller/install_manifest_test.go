package controller

import (
	"os"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"
)

func TestDevelopmentManagerManifestIsLocalOnlyAndRestricted(t *testing.T) {
	t.Parallel()
	deployment := readManifest[appsv1.Deployment](t, "../../config/manager/deployment.yaml")
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 {
		t.Fatalf("manager replicas = %#v", deployment.Spec.Replicas)
	}
	rolling := deployment.Spec.Strategy.RollingUpdate
	if deployment.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType || rolling == nil || rolling.MaxUnavailable == nil || *rolling.MaxUnavailable != intstr.FromInt32(0) || rolling.MaxSurge == nil || *rolling.MaxSurge != intstr.FromInt32(1) {
		t.Fatalf("manager rollout = %#v", deployment.Spec.Strategy)
	}
	pod := deployment.Spec.Template.Spec
	if pod.ServiceAccountName != "controller-manager" || pod.AutomountServiceAccountToken == nil || !*pod.AutomountServiceAccountToken || pod.EnableServiceLinks == nil || *pod.EnableServiceLinks {
		t.Fatalf("manager pod identity = %#v", pod)
	}
	if pod.NodeSelector[corev1.LabelOSStable] != "linux" || pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot || pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("manager pod security = %#v", pod.SecurityContext)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("manager containers = %#v", pod.Containers)
	}
	container := pod.Containers[0]
	for _, required := range []string{
		"--metrics-bind-address=0",
		"--leader-elect=true",
		"--webhook-enabled=false",
		"--orchestrator-image=pgshard/orchestrator:dev",
		"--pooler-image=pgshard/pooler:dev",
	} {
		if !slices.Contains(container.Args, required) {
			t.Errorf("manager args %q do not contain %q", container.Args, required)
		}
	}
	security := container.SecurityContext
	if container.Image != "pgshard/operator:dev" || container.ImagePullPolicy != corev1.PullIfNotPresent || security == nil || security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation || security.Capabilities == nil || !slices.Contains(security.Capabilities.Drop, corev1.Capability("ALL")) {
		t.Fatalf("manager container security = %#v", container)
	}
	if container.StartupProbe == nil || container.LivenessProbe == nil || container.ReadinessProbe == nil {
		t.Fatalf("manager probes = %#v", container)
	}
}

func TestDevelopmentKustomizationBindsOnlyTheManagerRuntime(t *testing.T) {
	t.Parallel()
	type kustomization struct {
		APIVersion string   `json:"apiVersion"`
		Kind       string   `json:"kind"`
		Namespace  string   `json:"namespace"`
		NamePrefix string   `json:"namePrefix"`
		Resources  []string `json:"resources"`
	}
	config := readManifest[kustomization](t, "../../config/development/kustomization.yaml")
	if config.APIVersion != "kustomize.config.k8s.io/v1beta1" || config.Kind != "Kustomization" || config.Namespace != "pgshard-system" || config.NamePrefix != "pgshard-" {
		t.Fatalf("development names = %#v", config)
	}
	resources := slices.Clone(config.Resources)
	wantedResources := []string{"namespace.yaml", "../crd", "../rbac", "../manager"}
	slices.Sort(resources)
	slices.Sort(wantedResources)
	if !slices.Equal(resources, wantedResources) {
		t.Fatalf("development resources = %q", config.Resources)
	}
	namespace := readManifest[corev1.Namespace](t, "../../config/development/namespace.yaml")
	for _, key := range []string{
		"pod-security.kubernetes.io/audit",
		"pod-security.kubernetes.io/enforce",
		"pod-security.kubernetes.io/warn",
	} {
		if namespace.Labels[key] != "restricted" || namespace.Labels[key+"-version"] != "v1.36" {
			t.Errorf("development namespace labels[%q] = %q, version = %q", key, namespace.Labels[key], namespace.Labels[key+"-version"])
		}
	}

	account := readManifest[corev1.ServiceAccount](t, "../../config/manager/service_account.yaml")
	if account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken {
		t.Fatalf("service account token default = %#v", account.AutomountServiceAccountToken)
	}
	binding := readManifest[rbacv1.ClusterRoleBinding](t, "../../config/rbac/role_binding.yaml")
	if binding.RoleRef.Kind != "ClusterRole" || binding.RoleRef.Name != "manager-role" || len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Name != "controller-manager" || binding.Subjects[0].Namespace != "system" {
		t.Fatalf("manager role binding = %#v", binding)
	}
	leaderRole := readManifest[rbacv1.Role](t, "../../config/rbac/leader_election_role.yaml")
	if leaderRole.Namespace != "system" || len(leaderRole.Rules) != 1 || !slices.Contains(leaderRole.Rules[0].APIGroups, "coordination.k8s.io") || !slices.Contains(leaderRole.Rules[0].Resources, "leases") {
		t.Fatalf("leader-election role = %#v", leaderRole)
	}
	leaderBinding := readManifest[rbacv1.RoleBinding](t, "../../config/rbac/leader_election_role_binding.yaml")
	if leaderBinding.Namespace != "system" || leaderBinding.RoleRef.Kind != "Role" || leaderBinding.RoleRef.Name != "leader-election-role" || len(leaderBinding.Subjects) != 1 || leaderBinding.Subjects[0].Name != "controller-manager" || leaderBinding.Subjects[0].Namespace != "system" {
		t.Fatalf("leader-election role binding = %#v", leaderBinding)
	}
}

func readManifest[T any](t *testing.T, path string) T {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var object T
	if err := yaml.UnmarshalStrict(contents, &object); err != nil {
		t.Fatal(err)
	}
	return object
}
