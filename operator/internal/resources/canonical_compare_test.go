package resources

import (
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func singleMemberPlannableCluster() *pgshardv1alpha1.PgShardCluster {
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
	return cluster
}

func multiMemberPlannableCluster() *pgshardv1alpha1.PgShardCluster {
	cluster := testCluster()
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
	cluster.Status.PostgreSQLReplicationCredentials = testPostgreSQLReplicationCredentials(cluster)
	return cluster
}

func planWorkloadTemplate(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster, name string) (*corev1.PodTemplateSpec, bool) {
	t.Helper()
	return planWorkloadTemplateWithRuntime(t, cluster, name, PostgreSQLRuntimeAgentQuarantine)
}

func planWorkloadTemplateWithRuntime(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster, name string, runtime PostgreSQLRuntime) (*corev1.PodTemplateSpec, bool) {
	t.Helper()
	images := DevelopmentImages()
	images.PostgreSQLRuntime = runtime
	plan, err := Plan(cluster, images)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range plan {
		switch workload := object.(type) {
		case *appsv1.StatefulSet:
			if workload.Name == name {
				return workload.Spec.Template.DeepCopy(), false
			}
		case *appsv1.Deployment:
			if workload.Name == name {
				return workload.Spec.Template.DeepCopy(), true
			}
		}
	}
	return nil, false
}

// testBinding is the authoritative binding evidence the simulated LIVE pods
// carry; LiveNormalForm validates against it.
func testBinding() *BindingEvidence {
	return &BindingEvidence{NodeName: "node-a", NodeUID: "node-uid-a", BootID: "boot-a", Zone: "zone-a", Region: "region-a"}
}

// stampAndAdmit returns a stamped parent template plus the pod the real
// controllers + API server would produce from it, simulating every mutation the
// contract normalizer must tolerate. It deliberately does NOT pre-apply the API
// server's field defaulting to the pod, so the test also proves the normalizer
// defaults both sides to convergence.
func stampAndAdmit(t *testing.T, template *corev1.PodTemplateSpec, nc NormContext, live bool) (metav1.ObjectMeta, corev1.PodSpec, metav1.ObjectMeta, corev1.PodSpec) {
	t.Helper()
	if _, err := ApplyContractStamp(template, nc.Class, "cluster-uid", nc.Shard, nc.Member, 1); err != nil {
		t.Fatal(err)
	}
	podMeta := *template.ObjectMeta.DeepCopy()
	podSpec := *template.Spec.DeepCopy()

	// The API server fills metadata.namespace from the request namespace.
	podMeta.Namespace = nc.Namespace
	// Deprecated serviceAccount mirror (ServiceAccount admission plugin).
	podSpec.DeprecatedServiceAccount = podSpec.ServiceAccountName
	// Priority admission resolves the pinned tuple.
	value, _ := priorityValueForClassName(podSpec.PriorityClassName)
	resolved := value
	policy := pgShardPreemptionPolicy
	podSpec.Priority = &resolved
	podSpec.PreemptionPolicy = &policy

	if podMeta.Labels == nil {
		podMeta.Labels = map[string]string{}
	}
	if isMemberClass(nc.Class) {
		name := PostgreSQLMemberStatefulSetName(nc.ClusterName, nc.Shard, nc.Member) + "-0"
		podMeta.Name = name
		podMeta.Labels["controller-revision-hash"] = name + "-abcde"
		podMeta.Labels["statefulset.kubernetes.io/pod-name"] = name
		podMeta.Labels["apps.kubernetes.io/pod-index"] = "0"
		podMeta.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: PostgreSQLMemberStatefulSetName(nc.ClusterName, nc.Shard, nc.Member), UID: "sts-uid", Controller: ptr(true)}}
		podSpec.Hostname = name
		podSpec.Subdomain = shardName(nc.ClusterName, nc.Shard)
	} else {
		podMeta.GenerateName = nc.ClusterName + "-x-"
		podMeta.Name = nc.ClusterName + "-x-abcde"
		podMeta.Labels["pod-template-hash"] = "77abcde"
		podMeta.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: nc.ClusterName + "-x", UID: "rs-uid", Controller: ptr(true)}}
	}

	// Orchestrator (automount=true) receives the injected projected token.
	if classExpectsInjectedToken(nc.Class) {
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{Name: "kube-api-access-zzzzz", VolumeSource: corev1.VolumeSource{Projected: expectedInjectedTokenVolumeSource()}})
		for i := range podSpec.Containers {
			podSpec.Containers[i].VolumeMounts = append(podSpec.Containers[i].VolumeMounts, corev1.VolumeMount{Name: "kube-api-access-zzzzz", ReadOnly: true, MountPath: serviceAccountTokenMountPath})
		}
	}

	if live {
		binding := testBinding()
		podSpec.NodeName = binding.NodeName
		if podMeta.Annotations == nil {
			podMeta.Annotations = map[string]string{}
		}
		podMeta.Annotations[PostgreSQLNodeUIDAnnotation] = binding.NodeUID
		podMeta.Annotations[PostgreSQLNodeBootIDAnnotation] = binding.BootID
		podMeta.Labels[corev1.LabelTopologyZone] = binding.Zone
		podMeta.Labels[corev1.LabelTopologyRegion] = binding.Region
	}
	return podMeta, podSpec, template.ObjectMeta, template.Spec
}

type contractCase struct {
	name    string
	cluster *pgshardv1alpha1.PgShardCluster
	object  string
	class   PodClass
	member  int32
}

func contractCases(t *testing.T) []contractCase {
	single := singleMemberPlannableCluster()
	multi := multiMemberPlannableCluster()
	return []contractCase{
		{"single-member", single, PostgreSQLMemberStatefulSetName(single.Name, 0, 0), ClassSingleMember, 0},
		{"pooler", single, single.Name + PoolerSuffix, ClassPooler, 0},
		{"orchestrator", single, single.Name + OrchestratorSuffix, ClassOrchestrator, 0},
		{"source", multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0), ClassSource, 0},
		{"standby", multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 1), ClassStandby, 1},
	}
}

func TestHonestPodsNormalizeEqualToStampedTemplates(t *testing.T) {
	t.Parallel()
	for _, test := range contractCases(t) {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			template, _ := planWorkloadTemplate(t, test.cluster, test.object)
			if template == nil {
				t.Fatalf("plan has no workload %s", test.object)
			}
			for _, live := range []bool{false, true} {
				nc := NormContext{Class: test.class, ClusterName: test.cluster.Name, Namespace: test.cluster.Namespace, Shard: 0, Member: test.member}
				stage := StageCreate
				if live {
					stage = StageLive
					nc.Binding = testBinding()
				}
				tmpl := template.DeepCopy()
				podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, tmpl, nc, live)
				if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, stage, false); err != nil {
					t.Fatalf("honest %s (live=%t) did not match: %v", test.name, live, err)
				}
				// The admit-side hash must equal the stamped hash for every class.
				stampHash, err := ComputeContractStamp(test.class, "cluster-uid", 0, test.member, 1, tmpl)
				if err != nil {
					t.Fatal(err)
				}
				admitHash, err := HashAdmittedPod(nc, podMeta, podSpec, stage, "cluster-uid", 1)
				if err != nil {
					t.Fatal(err)
				}
				if stampHash != admitHash {
					t.Fatalf("stamp hash != admit hash for %s (live=%t)", test.name, live)
				}
			}
		})
	}
}

func TestComparatorRejectsAdversarialMutations(t *testing.T) {
	t.Parallel()
	mutations := []struct {
		name   string
		want   string
		mutate func(*metav1.ObjectMeta, *corev1.PodSpec)
	}{
		{"foreign image", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) { s.Containers[0].Image = "evil/pg:latest" }},
		{"extra container", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Containers = append(s.Containers, corev1.Container{Name: "sidecar", Image: "x@sha256:" + strings.Repeat("a", 64)})
		}},
		{"ephemeral container", "ephemeral", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "x"}}}
		}},
		{"extra env", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Containers[0].Env = append(s.Containers[0].Env, corev1.EnvVar{Name: "PGSHARD_INJECT", Value: "1"})
		}},
		{"duplicate env", "duplicate env", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			if len(s.Containers[0].Env) == 0 {
				s.Containers[0].Env = []corev1.EnvVar{{Name: "A", Value: "1"}}
			}
			s.Containers[0].Env = append(s.Containers[0].Env, s.Containers[0].Env[0])
		}},
		{"valueFrom-smuggled env", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Containers[0].Env = append(s.Containers[0].Env, corev1.EnvVar{Name: "SNEAK", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}})
		}},
		{"non-nil command", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) { s.Containers[0].Command = []string{"/bin/sh"} }},
		{"non-nil args", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Containers[0].Args = []string{"--postgres-mode", "quarantine"}
		}},
		{"extra volume", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Volumes = append(s.Volumes, corev1.Volume{Name: "sneak", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		}},
		{"extra mount", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Containers[0].VolumeMounts = append(s.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "data", MountPath: "/sneak"})
		}},
		{"automount true", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) { s.AutomountServiceAccountToken = ptr(true) }},
		{"privileged", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Containers[0].SecurityContext.Privileged = ptr(true)
		}},
		{"hostPath volume", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Volumes = append(s.Volumes, corev1.Volume{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}})
		}},
		{"hostNetwork", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) { s.HostNetwork = true }},
		{"rewritten probe", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Containers[0].LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/evil"}}}}
		}},
		{"lifecycle hook", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/evil"}}}}
		}},
		{"extra toleration", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.Tolerations = append(s.Tolerations, corev1.Toleration{Key: "special", Operator: corev1.TolerationOpExists})
		}},
		{"wrong priorityClass", "does not match", func(_ *metav1.ObjectMeta, s *corev1.PodSpec) {
			s.PriorityClassName = "system-cluster-critical"
			s.Priority = ptr(int32(2000000000))
		}},
		{"extra label", "does not match", func(m *metav1.ObjectMeta, _ *corev1.PodSpec) {
			if m.Labels == nil {
				m.Labels = map[string]string{}
			}
			m.Labels["sneak"] = "1"
		}},
	}

	// Use the source class (a member with rich env/volumes) as the base target.
	cluster := multiMemberPlannableCluster()
	nc := NormContext{Class: ClassSource, ClusterName: cluster.Name, Namespace: cluster.Namespace, Shard: 0, Member: 0}
	object := PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0)
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			t.Parallel()
			template, _ := planWorkloadTemplate(t, cluster, object)
			if template == nil {
				t.Fatalf("plan has no workload %s", object)
			}
			podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, false)
			mutation.mutate(&podMeta, &podSpec)
			err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false)
			if err == nil || !strings.Contains(err.Error(), mutation.want) {
				t.Fatalf("mutation %q error = %v, want containing %q", mutation.name, err, mutation.want)
			}
		})
	}
}

// TestComparatorPinsSingleMemberCatalogServingTLS proves the activation-TLS
// parity property (v7 §9): the single-member catalog-serving pod's
// catalog-server-tls volume (its catalog Secret name + key projection), the
// postmaster ssl_cert_file/ssl_key_file arguments, and ssl=on are all inside the
// stamped canonical contract, so §1/§3 already pin them — a foreign catalog
// Secret name or altered catalog ssl arguments make the pod diverge from its
// stamped template and are DENIED by the comparator.
//
// NOTE: multi-member activation-TLS is DEFERRED — the shard-0 SOURCE pod has no
// catalog-server-tls volume (it does not serve the catalog directly), so this
// parity assertion is scoped to the single-member catalog-serving class.
func TestComparatorPinsSingleMemberCatalogServingTLS(t *testing.T) {
	t.Parallel()
	cluster := singleMemberPlannableCluster()
	object := PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0)
	// Direct runtime keeps the postmaster ssl_* arguments on the postgresql
	// container (agent-quarantine relocates the postmaster); the catalog-server-tls
	// volume is present in either runtime.
	baseTemplate, _ := planWorkloadTemplateWithRuntime(t, cluster, object, PostgreSQLRuntimeDirect)
	if baseTemplate == nil {
		t.Fatalf("plan has no single-member catalog-serving workload %s", object)
	}
	// Confirm the honest catalog-serving pod carries the pinned TLS surface, so the
	// mutations below are exercising real, present fields.
	assertHasCatalogServerTLSVolume(t, baseTemplate.Spec)
	assertHasCatalogSSLArgs(t, baseTemplate.Spec)

	nc := NormContext{Class: ClassSingleMember, ClusterName: cluster.Name, Namespace: cluster.Namespace, Shard: 0, Member: 0}
	mutations := []struct {
		name   string
		mutate func(*corev1.PodSpec)
	}{
		{"foreign catalog-server-tls secret name", func(s *corev1.PodSpec) {
			for i := range s.Volumes {
				if s.Volumes[i].Name == "catalog-server-tls" && s.Volumes[i].Secret != nil {
					s.Volumes[i].Secret.SecretName = "attacker-catalog-tls"
				}
			}
		}},
		{"removed catalog-server-tls volume", func(s *corev1.PodSpec) {
			kept := s.Volumes[:0]
			for _, volume := range s.Volumes {
				if volume.Name != "catalog-server-tls" {
					kept = append(kept, volume)
				}
			}
			s.Volumes = kept
		}},
		{"altered ssl_cert_file arg", func(s *corev1.PodSpec) {
			replaceContainerArgValue(s, "postgresql", "ssl_cert_file=/etc/pgshard/catalog-tls/tls.crt", "ssl_cert_file=/tmp/attacker.crt")
		}},
		{"altered ssl_key_file arg", func(s *corev1.PodSpec) {
			replaceContainerArgValue(s, "postgresql", "ssl_key_file=/etc/pgshard/catalog-tls/tls.key", "ssl_key_file=/tmp/attacker.key")
		}},
		{"ssl disabled", func(s *corev1.PodSpec) {
			replaceContainerArgValue(s, "postgresql", "ssl=on", "ssl=off")
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			t.Parallel()
			template := baseTemplate.DeepCopy()
			podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, false)
			mutation.mutate(&podSpec)
			err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false)
			if err == nil || !strings.Contains(err.Error(), "does not match") {
				t.Fatalf("catalog-TLS mutation %q error = %v, want a contract mismatch", mutation.name, err)
			}
		})
	}
}

func assertHasCatalogServerTLSVolume(t *testing.T, spec corev1.PodSpec) {
	t.Helper()
	for _, volume := range spec.Volumes {
		if volume.Name == "catalog-server-tls" && volume.Secret != nil && volume.Secret.SecretName != "" {
			return
		}
	}
	t.Fatal("honest single-member catalog-serving pod is missing its catalog-server-tls Secret volume")
}

func assertHasCatalogSSLArgs(t *testing.T, spec corev1.PodSpec) {
	t.Helper()
	for _, container := range spec.Containers {
		if container.Name != "postgresql" {
			continue
		}
		var haveCert, haveKey, haveOn bool
		for _, arg := range container.Args {
			switch arg {
			case "ssl_cert_file=/etc/pgshard/catalog-tls/tls.crt":
				haveCert = true
			case "ssl_key_file=/etc/pgshard/catalog-tls/tls.key":
				haveKey = true
			case "ssl=on":
				haveOn = true
			}
		}
		if haveCert && haveKey && haveOn {
			return
		}
		t.Fatalf("postgresql container missing pinned catalog ssl args: cert=%t key=%t on=%t", haveCert, haveKey, haveOn)
	}
	t.Fatal("honest single-member catalog-serving pod has no postgresql container")
}

func replaceContainerArgValue(spec *corev1.PodSpec, container, oldArg, newArg string) {
	for i := range spec.Containers {
		if spec.Containers[i].Name != container {
			continue
		}
		for j := range spec.Containers[i].Args {
			if spec.Containers[i].Args[j] == oldArg {
				spec.Containers[i].Args[j] = newArg
			}
		}
	}
}

func TestComparatorTokenTupleTamperingIsRejected(t *testing.T) {
	t.Parallel()
	cluster := singleMemberPlannableCluster()
	nc := NormContext{Class: ClassOrchestrator, ClusterName: cluster.Name, Namespace: cluster.Namespace}
	object := cluster.Name + OrchestratorSuffix
	for _, test := range []struct {
		name   string
		mutate func(*corev1.PodSpec)
	}{
		{"second projected token volume", func(s *corev1.PodSpec) {
			s.Volumes = append(s.Volumes, corev1.Volume{Name: "kube-api-access-yyyyy", VolumeSource: corev1.VolumeSource{Projected: expectedInjectedTokenVolumeSource()}})
		}},
		{"extra token source", func(s *corev1.PodSpec) {
			for i := range s.Volumes {
				if s.Volumes[i].Projected != nil {
					s.Volumes[i].Projected.Sources = append(s.Volumes[i].Projected.Sources, corev1.VolumeProjection{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "steal"}}})
				}
			}
		}},
		{"token mounted read-write", func(s *corev1.PodSpec) {
			for ci := range s.Containers {
				for mi := range s.Containers[ci].VolumeMounts {
					if strings.HasPrefix(s.Containers[ci].VolumeMounts[mi].Name, "kube-api-access-") {
						s.Containers[ci].VolumeMounts[mi].ReadOnly = false
					}
				}
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			template, _ := planWorkloadTemplate(t, cluster, object)
			if template == nil {
				t.Fatalf("plan has no workload %s", object)
			}
			podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, false)
			test.mutate(&podSpec)
			if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false); err == nil {
				t.Fatalf("token tuple tampering %q was accepted", test.name)
			}
		})
	}
}

func TestCreateNormalFormRejectsPresetNodeName(t *testing.T) {
	t.Parallel()
	cluster := multiMemberPlannableCluster()
	nc := NormContext{Class: ClassSource, ClusterName: cluster.Name, Namespace: cluster.Namespace, Shard: 0, Member: 0}
	template, _ := planWorkloadTemplate(t, cluster, PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0))
	podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, false)
	podSpec.NodeName = "attacker-node"
	if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false); err == nil || !strings.Contains(err.Error(), "created unassigned") {
		t.Fatalf("preset nodeName at create error = %v", err)
	}
}

func TestLiveNormalFormRejectsForeignBindingResidue(t *testing.T) {
	t.Parallel()
	cluster := multiMemberPlannableCluster()
	nc := NormContext{Class: ClassSource, ClusterName: cluster.Name, Namespace: cluster.Namespace, Shard: 0, Member: 0, Binding: testBinding()}
	template, _ := planWorkloadTemplate(t, cluster, PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0))
	podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, true)
	// A validated bound pod passes.
	if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageLive, false); err != nil {
		t.Fatalf("honest bound pod rejected: %v", err)
	}
	// An extra binding-copied label (not zone/region) is unexpected residue.
	foreign := podMeta.DeepCopy()
	foreign.Labels["topology.kubernetes.io/rack"] = "rack-3"
	if err := ComparePodToStampedTemplate(nc, *foreign, podSpec, templateMeta, templateSpec, StageLive, false); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("foreign binding label residue error = %v", err)
	}
	// Forged node-UID evidence is rejected.
	forgedUID := podMeta.DeepCopy()
	forgedUID.Annotations[PostgreSQLNodeUIDAnnotation] = "attacker-node-uid"
	if err := ComparePodToStampedTemplate(nc, *forgedUID, podSpec, templateMeta, templateSpec, StageLive, false); err == nil || !strings.Contains(err.Error(), "node-UID") {
		t.Fatalf("forged node-UID error = %v", err)
	}
	// A cross-node nodeName not matching the binding evidence is rejected.
	crossNode := podSpec.DeepCopy()
	crossNode.NodeName = "other-node"
	if err := ComparePodToStampedTemplate(nc, podMeta, *crossNode, templateMeta, templateSpec, StageLive, false); err == nil || !strings.Contains(err.Error(), "nodeName") {
		t.Fatalf("cross-node nodeName error = %v", err)
	}
}

func TestDigestPinEnforcedOnlyWhenActive(t *testing.T) {
	t.Parallel()
	cluster := singleMemberPlannableCluster()
	nc := NormContext{Class: ClassOrchestrator, ClusterName: cluster.Name, Namespace: cluster.Namespace}
	template, _ := planWorkloadTemplate(t, cluster, cluster.Name+OrchestratorSuffix)
	// DevelopmentImages orchestrator is a mutable :main tag.
	if ImageIsDigestPinned(template.Spec.Containers[0].Image) {
		t.Fatalf("expected a non-digest dev image, got %q", template.Spec.Containers[0].Image)
	}
	podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, false)
	// Isolation off: the :main image is tolerated.
	if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false); err != nil {
		t.Fatalf("dev :main image rejected with isolation off: %v", err)
	}
	// Isolation on: the non-digest image is refused.
	if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, true); err == nil || !strings.Contains(err.Error(), "not digest-pinned") {
		t.Fatalf("non-digest image under active isolation error = %v", err)
	}
}

func TestComparatorRelationalMetadataValidation(t *testing.T) {
	t.Parallel()
	multi := multiMemberPlannableCluster()
	single := singleMemberPlannableCluster()
	memberNC := NormContext{Class: ClassSource, ClusterName: multi.Name, Namespace: multi.Namespace, Shard: 0, Member: 0}
	poolerNC := NormContext{Class: ClassPooler, ClusterName: single.Name, Namespace: single.Namespace}
	for _, test := range []struct {
		name    string
		cluster *pgshardv1alpha1.PgShardCluster
		object  string
		nc      NormContext
		want    string
		mutate  func(*metav1.ObjectMeta)
	}{
		{"member carrying pod-template-hash", multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0), memberNC, "must not carry", func(m *metav1.ObjectMeta) {
			m.Labels["pod-template-hash"] = "abcde"
		}},
		{"member wrong pod-name", multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0), memberNC, "pod-name label", func(m *metav1.ObjectMeta) {
			m.Labels["statefulset.kubernetes.io/pod-name"] = "wrong-0"
		}},
		{"member missing revision hash", multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0), memberNC, "controller-revision-hash", func(m *metav1.ObjectMeta) {
			delete(m.Labels, "controller-revision-hash")
		}},
		{"member wrong owner kind", multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0), memberNC, "owner reference kind", func(m *metav1.ObjectMeta) {
			m.OwnerReferences[0].Kind = "ReplicaSet"
		}},
		{"member two controllers", multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0), memberNC, "exactly one controller", func(m *metav1.ObjectMeta) {
			m.OwnerReferences = append(m.OwnerReferences, metav1.OwnerReference{Kind: "StatefulSet", Name: "other", Controller: ptr(true)})
		}},
		{"wrong namespace", multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0), memberNC, "does not match its cluster namespace", func(m *metav1.ObjectMeta) {
			m.Namespace = "attacker"
		}},
		{"supporting carrying statefulset label", single, single.Name + PoolerSuffix, poolerNC, "must not carry", func(m *metav1.ObjectMeta) {
			m.Labels["statefulset.kubernetes.io/pod-name"] = "x-0"
		}},
		{"supporting missing pod-template-hash", single, single.Name + PoolerSuffix, poolerNC, "missing its", func(m *metav1.ObjectMeta) {
			delete(m.Labels, "pod-template-hash")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			template, _ := planWorkloadTemplate(t, test.cluster, test.object)
			if template == nil {
				t.Fatalf("plan has no workload %s", test.object)
			}
			podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, test.nc, false)
			test.mutate(&podMeta)
			if err := ComparePodToStampedTemplate(test.nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("relational metadata %q error = %v, want containing %q", test.name, err, test.want)
			}
		})
	}
}

func TestComparatorRejectsOwnerRefUIDMismatch(t *testing.T) {
	t.Parallel()
	multi := multiMemberPlannableCluster()
	nc := NormContext{Class: ClassSource, ClusterName: multi.Name, Namespace: multi.Namespace, Shard: 0, Member: 0, Provenance: &ControllerEvidence{ParentUID: "the-live-uid"}}
	template, _ := planWorkloadTemplate(t, multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0))
	podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, false)
	// stampAndAdmit set UID "sts-uid"; the evidence expects "the-live-uid".
	if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false); err == nil || !strings.Contains(err.Error(), "UID does not match") {
		t.Fatalf("owner-ref UID mismatch error = %v", err)
	}
}

func TestComparatorRejectsMissingPriorityTuple(t *testing.T) {
	t.Parallel()
	multi := multiMemberPlannableCluster()
	nc := NormContext{Class: ClassSource, ClusterName: multi.Name, Namespace: multi.Namespace, Shard: 0, Member: 0}
	template, _ := planWorkloadTemplate(t, multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0))
	podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, false)
	// A pod created without Priority admission carries no resolved priority.
	podSpec.Priority = nil
	if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false); err == nil || !strings.Contains(err.Error(), "priority does not match") {
		t.Fatalf("missing priority error = %v", err)
	}
	// A wrong resolved value is likewise rejected.
	podMeta2, podSpec2, tm2, ts2 := stampAndAdmit(t, planWorkloadTemplateMust(t, multi, PostgreSQLMemberStatefulSetName(multi.Name, 0, 0)), nc, false)
	podSpec2.PreemptionPolicy = nil
	if err := ComparePodToStampedTemplate(nc, podMeta2, podSpec2, tm2, ts2, StageCreate, false); err == nil || !strings.Contains(err.Error(), "preemptionPolicy does not match") {
		t.Fatalf("missing preemptionPolicy error = %v", err)
	}
}

func planWorkloadTemplateMust(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster, name string) *corev1.PodTemplateSpec {
	t.Helper()
	template, _ := planWorkloadTemplate(t, cluster, name)
	if template == nil {
		t.Fatalf("plan has no workload %s", name)
	}
	return template
}

func TestLimitOnlyResourcesConvergeInNormalForm(t *testing.T) {
	t.Parallel()
	// SetDefaults_Pod copies each missing request from its corresponding limit
	// on a real Pod but not on a template. The normalizer applies the same
	// relation to both sides so a limit-only entry (cpu/memory/ephemeral/
	// hugepage/extended) converges rather than false-denying.
	hugepage := corev1.ResourceName("hugepages-2Mi")
	extended := corev1.ResourceName("example.com/gpu")
	makeSpec := func(withRequests bool) corev1.PodSpec {
		requests := corev1.ResourceList{}
		if withRequests {
			requests = corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("2"),
				corev1.ResourceMemory:           resource.MustParse("4Gi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("8Gi"),
				hugepage:                        resource.MustParse("128Mi"),
				extended:                        resource.MustParse("1"),
			}
		}
		return corev1.PodSpec{Containers: []corev1.Container{{
			Name: "c",
			Resources: corev1.ResourceRequirements{
				Requests: requests,
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("4Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("8Gi"),
					hugepage:                        resource.MustParse("128Mi"),
					extended:                        resource.MustParse("1"),
				},
			},
		}}}
	}
	// The limit-only template and the API-server-defaulted (requests==limits)
	// pod must produce identical normal forms.
	templateSpec := makeSpec(false)
	podSpec := makeSpec(true)
	applyContractDefaults(&templateSpec)
	applyContractDefaults(&podSpec)
	if !equality.Semantic.DeepEqual(templateSpec, podSpec) {
		t.Fatalf("limit-only template did not converge with a requests-filled pod")
	}
	for name := range podSpec.Containers[0].Resources.Limits {
		if _, present := templateSpec.Containers[0].Resources.Requests[name]; !present {
			t.Fatalf("limit dimension %q was not copied into requests", name)
		}
	}
}

// TestComparatorConvergesRegardlessOfApiserverDefaulting proves blocker-1's
// stability property: the normalizer applies the pinned k8s-1.36 defaulting to
// both sides, so an honest pod matches its template whether or not that pod has
// already had API-server field defaulting applied.
//
// BOUNDARY: this exercises the enumerated default set against itself. The
// authority that the enumerated set matches the *target* API server is a real
// round-trip test (envtest for the stored parent StatefulSet/Deployment
// template; real KIND conformance goldens for the pod side), which is a
// follow-up when the envtest harness lands. A gap in the enumerated set
// false-denies honest pods (fail-closed), never opens.
func TestComparatorConvergesRegardlessOfApiserverDefaulting(t *testing.T) {
	t.Parallel()
	cluster := multiMemberPlannableCluster()
	nc := NormContext{Class: ClassSource, ClusterName: cluster.Name, Namespace: cluster.Namespace, Shard: 0, Member: 0}
	template := planWorkloadTemplateMust(t, cluster, PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0))
	podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, template, nc, false)
	// Undefaulted pod converges (normalizer defaults both sides).
	if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, StageCreate, false); err != nil {
		t.Fatalf("undefaulted pod rejected: %v", err)
	}
	// Fully API-defaulted pod also converges (idempotent).
	defaulted := podSpec.DeepCopy()
	applyContractDefaults(defaulted)
	if err := ComparePodToStampedTemplate(nc, podMeta, *defaulted, templateMeta, templateSpec, StageCreate, false); err != nil {
		t.Fatalf("API-defaulted pod rejected: %v", err)
	}
}

var _ = client.Object(nil)
