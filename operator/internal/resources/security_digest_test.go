package resources

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func securityDigestBaseTemplate() *corev1.PodTemplateSpec {
	runAsNonRoot := true
	return &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"a": "b"}, Annotations: map[string]string{"note": "x"}},
		Spec: corev1.PodSpec{
			ServiceAccountName: "pgshard-agent",
			SecurityContext:    &corev1.PodSecurityContext{RunAsNonRoot: &runAsNonRoot},
			Volumes: []corev1.Volume{{
				Name:         "catalog-server-tls",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "catalog-tls"}},
			}},
			Containers: []corev1.Container{{
				Name:            "postgresql",
				Image:           "pg@sha256:aaaa",
				Args:            []string{"-c", "ssl=on"},
				SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				VolumeMounts:    []corev1.VolumeMount{{Name: "catalog-server-tls", MountPath: "/tls"}},
				Env:             []corev1.EnvVar{{Name: "OTEL_ENDPOINT", Value: "http://collector-a:4317"}},
				Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}},
			}},
		},
	}
}

func TestComputeSecurityContractDigestIgnoresBenignChanges(t *testing.T) {
	t.Parallel()
	base := securityDigestBaseTemplate()
	baseDigest := ComputeSecurityContractDigest(base)
	if baseDigest == "" || len(baseDigest) != 64 {
		t.Fatalf("base security digest malformed: %q", baseDigest)
	}

	// BENIGN: an OpenTelemetry endpoint (env) change must NOT change the security
	// digest — so it does not force a security-generation bump / revocation.
	otel := base.DeepCopy()
	otel.Spec.Containers[0].Env[0].Value = "http://collector-b:4317"
	if got := ComputeSecurityContractDigest(otel); got != baseDigest {
		t.Fatalf("OTel env change changed the security digest (would force a needless revocation): %q != %q", got, baseDigest)
	}

	// BENIGN: a resource-request change must not change the security digest.
	resources := base.DeepCopy()
	resources.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("250m")
	if got := ComputeSecurityContractDigest(resources); got != baseDigest {
		t.Fatalf("resource change changed the security digest: %q != %q", got, baseDigest)
	}

	// BENIGN: annotation/label churn is ignored.
	labels := base.DeepCopy()
	labels.ObjectMeta.Annotations["note"] = "changed"
	labels.ObjectMeta.Labels["a"] = "c"
	if got := ComputeSecurityContractDigest(labels); got != baseDigest {
		t.Fatalf("metadata churn changed the security digest: %q != %q", got, baseDigest)
	}
}

func TestComputeSecurityContractDigestDetectsStrengthening(t *testing.T) {
	t.Parallel()
	base := securityDigestBaseTemplate()
	baseDigest := ComputeSecurityContractDigest(base)

	for name, mutate := range map[string]func(*corev1.PodTemplateSpec){
		"image": func(t *corev1.PodTemplateSpec) { t.Spec.Containers[0].Image = "pg@sha256:bbbb" },
		"catalog secret name": func(t *corev1.PodTemplateSpec) {
			t.Spec.Volumes[0].Secret.SecretName = "attacker-tls"
		},
		"ssl arg": func(t *corev1.PodTemplateSpec) { t.Spec.Containers[0].Args[1] = "ssl=off" },
		"dropped capability": func(t *corev1.PodTemplateSpec) {
			t.Spec.Containers[0].SecurityContext.Capabilities.Drop = nil
		},
		"service account": func(t *corev1.PodTemplateSpec) { t.Spec.ServiceAccountName = "attacker" },
		"host network":    func(t *corev1.PodTemplateSpec) { t.Spec.HostNetwork = true },
		"added mount": func(t *corev1.PodTemplateSpec) {
			t.Spec.Containers[0].VolumeMounts = append(t.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "x", MountPath: "/x"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			strengthened := base.DeepCopy()
			mutate(strengthened)
			if got := ComputeSecurityContractDigest(strengthened); got == baseDigest {
				t.Fatalf("security-strengthening change %q did not change the digest", name)
			}
		})
	}
}
