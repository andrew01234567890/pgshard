package controller

import (
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

func TestAdmissionOverlayEnablesOnlyTheSelfManagedWebhookRuntime(t *testing.T) {
	t.Parallel()
	type kustomization struct {
		APIVersion string   `json:"apiVersion"`
		Kind       string   `json:"kind"`
		Namespace  string   `json:"namespace"`
		NamePrefix string   `json:"namePrefix"`
		Resources  []string `json:"resources"`
		Patches    []struct {
			Path string `json:"path"`
		} `json:"patches"`
	}
	config := readManifest[kustomization](t, "../../config/admission/kustomization.yaml")
	resources := slices.Clone(config.Resources)
	wantedResources := []string{"../namespace", "../crd", "../rbac", "../manager", "../webhook", "rbac"}
	slices.Sort(resources)
	slices.Sort(wantedResources)
	if config.APIVersion != "kustomize.config.k8s.io/v1beta1" || config.Kind != "Kustomization" || config.Namespace != "pgshard-system" || config.NamePrefix != "pgshard-" || !slices.Equal(resources, wantedResources) || len(config.Patches) != 1 || config.Patches[0].Path != "manager_patch.yaml" {
		t.Fatalf("admission Kustomization = %#v", config)
	}

	patch := readManifest[appsv1.Deployment](t, "../../config/admission/manager_patch.yaml")
	if patch.Spec.ProgressDeadlineSeconds == nil || *patch.Spec.ProgressDeadlineSeconds != 180 || patch.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *patch.Spec.Template.Spec.TerminationGracePeriodSeconds != 20 || len(patch.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("admission manager patch = %#v", patch.Spec)
	}
	if patch.Spec.Template.Labels["pgshard.io/webhook-contract"] != "receipt-v1" {
		t.Fatalf("admission manager webhook contract = %#v", patch.Spec.Template.Labels)
	}
	container := patch.Spec.Template.Spec.Containers[0]
	for _, wanted := range []string{
		"--webhook-enabled=true",
		"--webhook-namespace=$(POD_NAMESPACE)",
		"--webhook-service-name=pgshard-webhook-service",
		"--webhook-ca-secret-name=pgshard-webhook-ca",
		"--webhook-serving-secret-name=pgshard-webhook-certificate",
		"--webhook-fencing-key-secret-name=pgshard-webhook-fencing-key",
		"--webhook-mutating-configuration-name=pgshard-mutating-webhook-configuration",
		"--webhook-validating-configuration-name=pgshard-validating-webhook-configuration",
		"--webhook-cert-dir=/run/pgshard/webhook",
	} {
		if !slices.Contains(container.Args, wanted) {
			t.Errorf("admission manager args %q do not contain %q", container.Args, wanted)
		}
	}
	if len(container.Env) != 1 || container.Env[0].Name != "POD_NAMESPACE" || container.Env[0].ValueFrom == nil || container.Env[0].ValueFrom.FieldRef == nil || container.Env[0].ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
		t.Fatalf("admission namespace environment = %#v", container.Env)
	}
	if len(container.Ports) != 1 || container.Ports[0].Name != "webhook" || container.Ports[0].ContainerPort != 9443 || container.StartupProbe == nil || container.StartupProbe.FailureThreshold != 75 {
		t.Fatalf("admission manager listener/probe = %#v / %#v", container.Ports, container.StartupProbe)
	}
	if container.Lifecycle == nil || container.Lifecycle.PreStop == nil || container.Lifecycle.PreStop.Sleep == nil || container.Lifecycle.PreStop.Sleep.Seconds != 5 {
		t.Fatalf("admission manager termination drain = %#v", container.Lifecycle)
	}
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].Name != "webhook-certificates" || container.VolumeMounts[0].MountPath != "/run/pgshard" || len(patch.Spec.Template.Spec.Volumes) != 1 || patch.Spec.Template.Spec.Volumes[0].EmptyDir == nil || patch.Spec.Template.Spec.Volumes[0].EmptyDir.Medium != corev1.StorageMediumMemory || patch.Spec.Template.Spec.Volumes[0].EmptyDir.SizeLimit == nil || patch.Spec.Template.Spec.Volumes[0].EmptyDir.SizeLimit.String() != "16Mi" {
		t.Fatalf("admission certificate volume = %#v / %#v", container.VolumeMounts, patch.Spec.Template.Spec.Volumes)
	}
}

func TestAdmissionResourcesArePrecreatedAndExactlyScoped(t *testing.T) {
	t.Parallel()
	type webhookKustomization struct {
		APIVersion string   `json:"apiVersion"`
		Kind       string   `json:"kind"`
		Resources  []string `json:"resources"`
		Patches    []struct {
			Path string `json:"path"`
		} `json:"patches"`
	}
	webhookConfig := readManifest[webhookKustomization](t, "../../config/webhook/kustomization.yaml")
	if webhookConfig.APIVersion != "kustomize.config.k8s.io/v1beta1" || webhookConfig.Kind != "Kustomization" || len(webhookConfig.Resources) != 5 || len(webhookConfig.Patches) != 2 || webhookConfig.Patches[0].Path != "mutating_selectors_patch.yaml" || webhookConfig.Patches[1].Path != "validating_selectors_patch.yaml" {
		t.Fatalf("webhook Kustomization patches = %#v", webhookConfig.Patches)
	}
	for _, item := range []struct {
		path       string
		name       string
		secretType corev1.SecretType
	}{
		{path: "../../config/webhook/ca_secret.yaml", name: "webhook-ca", secretType: corev1.SecretTypeOpaque},
		{path: "../../config/webhook/serving_secret.yaml", name: "webhook-certificate", secretType: corev1.SecretTypeOpaque},
		{path: "../../config/webhook/fencing_key_secret.yaml", name: "webhook-fencing-key", secretType: corev1.SecretTypeOpaque},
	} {
		secret := readManifest[corev1.Secret](t, item.path)
		if secret.Name != item.name || secret.Namespace != "system" || secret.Type != item.secretType || len(secret.Data) != 0 || secret.Labels["app.kubernetes.io/managed-by"] != "pgshard-operator" {
			t.Errorf("pre-created Secret %s = %#v", item.path, secret)
		}
	}
	service := readManifest[corev1.Service](t, "../../config/webhook/service.yaml")
	if service.Name != "webhook-service" || service.Namespace != "system" || len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 9444 || service.Spec.Ports[0].TargetPort != intstr.FromString("webhook") || service.Spec.Selector["pgshard.io/webhook-contract"] != "receipt-v1" {
		t.Fatalf("webhook Service = %#v", service)
	}

	secretRole := readManifest[rbacv1.Role](t, "../../config/admission/rbac/certificate_role.yaml")
	if secretRole.Namespace != "system" || len(secretRole.Rules) != 1 || !slices.Equal(secretRole.Rules[0].ResourceNames, []string{"pgshard-webhook-ca", "pgshard-webhook-certificate", "pgshard-webhook-fencing-key"}) || !slices.Equal(secretRole.Rules[0].Verbs, []string{"get", "update"}) {
		t.Fatalf("webhook Secret Role = %#v", secretRole)
	}
	configurationRole := readManifest[rbacv1.ClusterRole](t, "../../config/admission/rbac/configuration_role.yaml")
	if len(configurationRole.Rules) != 2 || !slices.Equal(configurationRole.Rules[0].ResourceNames, []string{"pgshard-mutating-webhook-configuration"}) || !slices.Equal(configurationRole.Rules[0].Verbs, []string{"get", "patch"}) || !slices.Equal(configurationRole.Rules[1].ResourceNames, []string{"pgshard-validating-webhook-configuration"}) || !slices.Equal(configurationRole.Rules[1].Verbs, []string{"get", "patch"}) {
		t.Fatalf("webhook configuration ClusterRole = %#v", configurationRole)
	}
	secretBinding := readManifest[rbacv1.RoleBinding](t, "../../config/admission/rbac/certificate_role_binding.yaml")
	configurationBinding := readManifest[rbacv1.ClusterRoleBinding](t, "../../config/admission/rbac/configuration_role_binding.yaml")
	if secretBinding.RoleRef.Kind != "Role" || secretBinding.RoleRef.Name != "webhook-certificate-role" || len(secretBinding.Subjects) != 1 || secretBinding.Subjects[0].Name != "controller-manager" || configurationBinding.RoleRef.Kind != "ClusterRole" || configurationBinding.RoleRef.Name != "webhook-configuration-role" || len(configurationBinding.Subjects) != 1 || configurationBinding.Subjects[0].Name != "controller-manager" {
		t.Fatalf("webhook RBAC bindings = %#v / %#v", secretBinding, configurationBinding)
	}
	mutatingSelectors := readManifest[admissionregistrationv1.MutatingWebhookConfiguration](t, "../../config/webhook/mutating_selectors_patch.yaml")
	if len(mutatingSelectors.Webhooks) != 3 || mutatingSelectors.Webhooks[0].Name != podfence.BindingWebhookName || mutatingSelectors.Webhooks[0].NamespaceSelector == nil || mutatingSelectors.Webhooks[0].NamespaceSelector.MatchLabels[podfence.NamespaceLabel] != podfence.NamespaceLabelValue || mutatingSelectors.Webhooks[1].Name != podfence.StatusWebhookName || !selectsFencingNamespace(mutatingSelectors.Webhooks[1].NamespaceSelector) || mutatingSelectors.Webhooks[2].Name != podfence.HandshakeWebhookName || !selectsFencingNamespace(mutatingSelectors.Webhooks[2].NamespaceSelector) {
		t.Fatalf("mutating webhook selector patch = %#v", mutatingSelectors.Webhooks)
	}
	validatingSelectors := readManifest[admissionregistrationv1.ValidatingWebhookConfiguration](t, "../../config/webhook/validating_selectors_patch.yaml")
	if len(validatingSelectors.Webhooks) != 10 ||
		validatingSelectors.Webhooks[0].Name != podfence.MetadataWebhookName || !selectsFencingNamespace(validatingSelectors.Webhooks[0].NamespaceSelector) ||
		validatingSelectors.Webhooks[1].Name != podfence.NamespaceWebhookName || !selectsFencingNamespace(validatingSelectors.Webhooks[1].ObjectSelector) ||
		validatingSelectors.Webhooks[2].Name != podfence.EnforcingNamespaceWebhookName || !selectsEnforcingLabelKey(validatingSelectors.Webhooks[2].ObjectSelector) ||
		validatingSelectors.Webhooks[3].Name != podfence.StatusValidationWebhookName || !selectsFencingNamespace(validatingSelectors.Webhooks[3].NamespaceSelector) ||
		validatingSelectors.Webhooks[4].Name != podfence.BindingValidationWebhookName || !selectsFencingNamespace(validatingSelectors.Webhooks[4].NamespaceSelector) ||
		validatingSelectors.Webhooks[5].Name != podfence.PodCreateWebhookName || !selectsFencingNamespace(validatingSelectors.Webhooks[5].NamespaceSelector) ||
		validatingSelectors.Webhooks[6].Name != podfence.WorkloadWebhookName || !selectsEnforcingIsolationNamespace(validatingSelectors.Webhooks[6].NamespaceSelector) ||
		validatingSelectors.Webhooks[7].Name != podfence.PodConnectFencedWebhookName || !selectsEnforcingIsolationNamespace(validatingSelectors.Webhooks[7].NamespaceSelector) ||
		validatingSelectors.Webhooks[8].Name != podfence.PodConnectManagerWebhookName || !selectsOperatorNamespace(validatingSelectors.Webhooks[8].NamespaceSelector) ||
		validatingSelectors.Webhooks[9].Name != podfence.LimitRangeWebhookName || !selectsEnforcingIsolationNamespace(validatingSelectors.Webhooks[9].NamespaceSelector) {
		t.Fatalf("validating webhook selector patch = %#v", validatingSelectors.Webhooks)
	}
}

func selectsOperatorNamespace(selector *metav1.LabelSelector) bool {
	return selector != nil && selector.MatchLabels["kubernetes.io/metadata.name"] == "pgshard-system" && len(selector.MatchLabels) == 1 && len(selector.MatchExpressions) == 0
}

// selectsEnforcingLabelKey matches the enforcing-namespace webhook's objectSelector,
// which fires for ANY namespace carrying the enforcing label KEY (any value) so a
// bogus value cannot be planted on an un-fenced namespace and later wedge activation.
func selectsEnforcingLabelKey(selector *metav1.LabelSelector) bool {
	return selector != nil && len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 1 &&
		selector.MatchExpressions[0].Key == podfence.NamespaceEnforcingLabel &&
		selector.MatchExpressions[0].Operator == metav1.LabelSelectorOpExists &&
		len(selector.MatchExpressions[0].Values) == 0
}

func TestGeneratedWebhookConfigurationsStayFailClosedAndBounded(t *testing.T) {
	t.Parallel()
	contents, err := os.Open("../../config/webhook/manifests.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer contents.Close()
	decoder := yamlutil.NewYAMLOrJSONDecoder(contents, 4096)
	mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := decoder.Decode(mutating); err != nil {
		t.Fatal(err)
	}
	validating := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := decoder.Decode(validating); err != nil {
		t.Fatal(err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected third webhook manifest: %v", err)
	}
	if len(mutating.Webhooks) != 4 || len(validating.Webhooks) != 13 {
		t.Fatalf("generated webhooks = %#v / %#v", mutating.Webhooks, validating.Webhooks)
	}
	activationFound := false
	for _, webhook := range validating.Webhooks {
		if webhook.Name != "vpgshardcatalogactivation.kb.io" {
			continue
		}
		activationFound = true
		if len(webhook.Rules) != 1 || !slices.Equal(webhook.Rules[0].Rule.Resources, []string{"pgshardcatalogactivations", "pgshardcatalogactivations/status"}) || !slices.Equal(webhook.Rules[0].Operations, []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update}) {
			t.Fatalf("catalog activation webhook coverage = %#v", webhook.Rules)
		}
	}
	if !activationFound {
		t.Fatal("catalog activation validating webhook is missing")
	}
	for _, webhook := range mutating.Webhooks {
		assertWebhookPolicy(t, webhook.ClientConfig, webhook.FailurePolicy, webhook.MatchPolicy, webhook.TimeoutSeconds)
	}
	for _, webhook := range validating.Webhooks {
		assertWebhookPolicy(t, webhook.ClientConfig, webhook.FailurePolicy, webhook.MatchPolicy, webhook.TimeoutSeconds)
	}
}

func assertWebhookPolicy(t *testing.T, clientConfig admissionregistrationv1.WebhookClientConfig, failurePolicy *admissionregistrationv1.FailurePolicyType, matchPolicy *admissionregistrationv1.MatchPolicyType, timeout *int32) {
	t.Helper()
	if clientConfig.Service == nil || clientConfig.Service.Name != "webhook-service" || clientConfig.Service.Namespace != "system" || clientConfig.Service.Port == nil || *clientConfig.Service.Port != 9444 || failurePolicy == nil || *failurePolicy != admissionregistrationv1.Fail || matchPolicy == nil || *matchPolicy != admissionregistrationv1.Equivalent || timeout == nil || *timeout != 5 {
		t.Fatalf("webhook policy = client %#v failure %#v match %#v timeout %#v", clientConfig, failurePolicy, matchPolicy, timeout)
	}
}

func selectsFencingNamespace(selector *metav1.LabelSelector) bool {
	return selector != nil && selector.MatchLabels[podfence.NamespaceLabel] == podfence.NamespaceLabelValue && len(selector.MatchLabels) == 1 && len(selector.MatchExpressions) == 0
}

// selectsEnforcingIsolationNamespace matches every genuinely-new isolation
// webhook's selector (WorkloadIntegrity, PodConnect, LimitRange), which requires
// BOTH the fencing label AND the isolation-enforcing label, so it fires only for a
// namespace whose isolation is in a non-INACTIVE phase (QUIESCE/RECREATE/ACTIVE).
func selectsEnforcingIsolationNamespace(selector *metav1.LabelSelector) bool {
	return selector != nil &&
		selector.MatchLabels[podfence.NamespaceLabel] == podfence.NamespaceLabelValue &&
		selector.MatchLabels[podfence.NamespaceEnforcingLabel] == podfence.NamespaceEnforcingLabelValue &&
		len(selector.MatchLabels) == 2 && len(selector.MatchExpressions) == 0
}

func TestManagerTokenRequestPolicyShape(t *testing.T) {
	t.Parallel()
	const policyPath = "../../config/admission/validatingadmissionpolicy_tokenrequest.yaml"
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	// The policy is namespace-independent by design: it identifies the manager
	// token by service-account name cluster-wide, so it must not hardcode any
	// installation namespace that a namespace change would leave stale and
	// fail-open.
	if strings.Contains(string(raw), "pgshard-system") {
		t.Fatalf("TokenRequest policy hardcodes an installation namespace")
	}
	file, err := os.Open(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := yamlutil.NewYAMLOrJSONDecoder(file, 4096)

	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{}
	if err := decoder.Decode(policy); err != nil {
		t.Fatal(err)
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
	if err := decoder.Decode(binding); err != nil {
		t.Fatal(err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected third TokenRequest policy document: %v", err)
	}

	if policy.Name != "pgshard-manager-tokenrequest" || policy.Spec.FailurePolicy == nil || *policy.Spec.FailurePolicy != admissionregistrationv1.Fail {
		t.Fatalf("policy identity/failurePolicy = %#v", policy.Spec.FailurePolicy)
	}
	if policy.Spec.MatchConstraints == nil || len(policy.Spec.MatchConstraints.ResourceRules) != 1 {
		t.Fatalf("policy matchConstraints = %#v", policy.Spec.MatchConstraints)
	}
	rule := policy.Spec.MatchConstraints.ResourceRules[0]
	if !slices.Contains(rule.APIGroups, "") || !slices.Contains(rule.Resources, "serviceaccounts/token") ||
		!slices.Contains(rule.Operations, admissionregistrationv1.Create) {
		t.Fatalf("policy resource rule = %#v", rule)
	}

	expressions := ""
	for _, validation := range policy.Spec.Validations {
		expressions += validation.Expression + "\n"
	}
	for _, predicate := range []string{
		"request.userInfo.username.startsWith('system:node:')",
		"'system:nodes' in request.userInfo.groups",
		"object.spec.boundObjectRef.kind == 'Pod'",
		"object.spec.audiences",
		"object.spec.expirationSeconds == 3607",
		"object.spec.expirationSeconds <= 3600",
		"request.name == 'pgshard-controller-manager'",
	} {
		if !strings.Contains(expressions+policyVariableExpressions(policy), predicate) {
			t.Fatalf("policy validations are missing predicate %q", predicate)
		}
	}

	if binding.Spec.PolicyName != policy.Name || !slices.Contains(binding.Spec.ValidationActions, admissionregistrationv1.Deny) {
		t.Fatalf("policy binding = %#v", binding.Spec)
	}
	// The binding must select every namespace (no namespace-scoping) so a
	// non-default installation namespace cannot escape the policy.
	if binding.Spec.MatchResources != nil && binding.Spec.MatchResources.NamespaceSelector != nil &&
		len(binding.Spec.MatchResources.NamespaceSelector.MatchLabels)+len(binding.Spec.MatchResources.NamespaceSelector.MatchExpressions) != 0 {
		t.Fatalf("policy binding is namespace-scoped: %#v", binding.Spec.MatchResources)
	}
}

func policyVariableExpressions(policy *admissionregistrationv1.ValidatingAdmissionPolicy) string {
	joined := ""
	for _, variable := range policy.Spec.Variables {
		joined += variable.Expression + "\n"
	}
	return joined
}
