package resources

import (
	"regexp"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func sampleTemplate() *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"app.kubernetes.io/component": "postgresql", "pgshard.io/shard": "0000", "pgshard.io/member": "0000"},
			Annotations: map[string]string{"pgshard.io/config-hash": "abc"},
			Finalizers:  []string{"pgshard.io/postgresql-termination"},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:           "example-shard-0000",
			AutomountServiceAccountToken: ptr(false),
			Containers: []corev1.Container{{
				Name:  "postgresql",
				Image: "registry.example/pg@sha256:" + repeat('a', 64),
				Env:   []corev1.EnvVar{{Name: "PGSHARD_POSTGRES_MODE", Value: "replication-standby"}},
				Ports: []corev1.ContainerPort{{Name: "postgresql", ContainerPort: 5432, Protocol: corev1.ProtocolTCP}},
			}},
		},
	}
}

func repeat(c byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = c
	}
	return string(out)
}

func TestComputeContractStampIsDeterministicAndSelfConsistent(t *testing.T) {
	t.Parallel()
	template := sampleTemplate()
	first, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, template)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, template)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("hash is not deterministic: %q vs %q", first, second)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(first) {
		t.Fatalf("hash is not 64 lowercase hex: %q", first)
	}

	// Apply the stamp (writes both annotations), then recompute over the stamped
	// template — the hash must be unchanged (the contract-hash annotation is
	// excluded from its own input, and the security-generation annotation is
	// re-derived from the authoritative argument).
	stamped := template.DeepCopy()
	applied, err := ApplyContractStamp(stamped, ClassStandby, "cluster-uid", 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if applied != first {
		t.Fatalf("ApplyContractStamp hash %q != ComputeContractStamp hash %q", applied, first)
	}
	if stamped.Annotations[PodContractHashAnnotation] != first {
		t.Fatalf("stamped contract-hash annotation = %q, want %q", stamped.Annotations[PodContractHashAnnotation], first)
	}
	if stamped.Annotations[PodSecurityGenerationAnnotation] != "1" {
		t.Fatalf("stamped security-generation annotation = %q, want 1", stamped.Annotations[PodSecurityGenerationAnnotation])
	}
	recomputed, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, stamped)
	if err != nil {
		t.Fatal(err)
	}
	if recomputed != first {
		t.Fatalf("recompute over stamped template = %q, want self-consistent %q", recomputed, first)
	}
}

func TestComputeContractStampExcludesOnlyItsOwnAnnotationFromInput(t *testing.T) {
	t.Parallel()
	template := sampleTemplate()
	base, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, template)
	if err != nil {
		t.Fatal(err)
	}

	// A different pre-existing contract-hash annotation must not change the hash.
	withStaleHash := template.DeepCopy()
	withStaleHash.Annotations[PodContractHashAnnotation] = repeat('f', 64)
	staleHashResult, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, withStaleHash)
	if err != nil {
		t.Fatal(err)
	}
	if staleHashResult != base {
		t.Fatal("contract-hash annotation was not excluded from its own input")
	}

	// A stale security-generation annotation must not change the hash (it is
	// re-derived from the authoritative argument).
	withStaleGen := template.DeepCopy()
	withStaleGen.Annotations[PodSecurityGenerationAnnotation] = "999"
	staleGenResult, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, withStaleGen)
	if err != nil {
		t.Fatal(err)
	}
	if staleGenResult != base {
		t.Fatal("security-generation annotation was not re-derived from the argument")
	}
}

func TestComputeContractStampDetectsTampering(t *testing.T) {
	t.Parallel()
	base, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, sampleTemplate())
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*corev1.PodTemplateSpec){
		"image": func(t *corev1.PodTemplateSpec) {
			t.Spec.Containers[0].Image = "registry.example/evil@sha256:" + repeat('b', 64)
		},
		"env value": func(t *corev1.PodTemplateSpec) { t.Spec.Containers[0].Env[0].Value = "quarantine" },
		"extra env": func(t *corev1.PodTemplateSpec) {
			t.Spec.Containers[0].Env = append(t.Spec.Containers[0].Env, corev1.EnvVar{Name: "X", Value: "y"})
		},
		"service acc": func(t *corev1.PodTemplateSpec) { t.Spec.ServiceAccountName = "attacker" },
		"automount":   func(t *corev1.PodTemplateSpec) { t.Spec.AutomountServiceAccountToken = ptr(true) },
		"label":       func(t *corev1.PodTemplateSpec) { t.Labels["pgshard.io/member"] = "0001" },
		"finalizer":   func(t *corev1.PodTemplateSpec) { t.Finalizers = nil },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tampered := sampleTemplate()
			mutate(tampered)
			got, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, tampered)
			if err != nil {
				t.Fatal(err)
			}
			if got == base {
				t.Fatalf("tampered template (%s) produced the same hash", name)
			}
		})
	}
}

func TestComputeContractStampIsDomainSeparated(t *testing.T) {
	t.Parallel()
	template := sampleTemplate()
	base, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, template)
	if err != nil {
		t.Fatal(err)
	}
	for name, hash := range map[string]func() (string, error){
		"class":      func() (string, error) { return ComputeContractStamp(ClassSource, "cluster-uid", 0, 1, 1, template) },
		"clusterUID": func() (string, error) { return ComputeContractStamp(ClassStandby, "other-uid", 0, 1, 1, template) },
		"shard":      func() (string, error) { return ComputeContractStamp(ClassStandby, "cluster-uid", 1, 1, 1, template) },
		"member":     func() (string, error) { return ComputeContractStamp(ClassStandby, "cluster-uid", 0, 2, 1, template) },
		"generation": func() (string, error) { return ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 2, template) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := hash()
			if err != nil {
				t.Fatal(err)
			}
			if got == base {
				t.Fatalf("domain component %s did not separate the hash", name)
			}
		})
	}
}

func TestComputeContractStampRejectsNilTemplate(t *testing.T) {
	t.Parallel()
	if _, err := ComputeContractStamp(ClassStandby, "cluster-uid", 0, 1, 1, nil); err == nil {
		t.Fatal("nil template must be rejected")
	}
}
