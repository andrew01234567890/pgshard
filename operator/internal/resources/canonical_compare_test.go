package resources

import (
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	images := DevelopmentImages()
	images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine
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

// stampAndAdmit returns a stamped parent template plus the pod the real
// controllers + API server would produce from it, simulating every mutation the
// contract normalizer must tolerate.
func stampAndAdmit(t *testing.T, template *corev1.PodTemplateSpec, nc NormContext, live bool) (metav1.ObjectMeta, corev1.PodSpec, metav1.ObjectMeta, corev1.PodSpec) {
	t.Helper()
	if _, err := ApplyContractStamp(template, nc.Class, "cluster-uid", nc.Shard, nc.Member, 1); err != nil {
		t.Fatal(err)
	}
	podMeta := *template.ObjectMeta.DeepCopy()
	podSpec := *template.Spec.DeepCopy()

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
		podSpec.NodeName = "node-a"
		if podMeta.Annotations == nil {
			podMeta.Annotations = map[string]string{}
		}
		podMeta.Annotations[PostgreSQLNodeUIDAnnotation] = "node-uid-a"
		podMeta.Annotations[PostgreSQLNodeBootIDAnnotation] = "boot-a"
		podMeta.Labels[corev1.LabelTopologyZone] = "zone-a"
		podMeta.Labels[corev1.LabelTopologyRegion] = "region-a"
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
			nc := NormContext{Class: test.class, ClusterName: test.cluster.Name, Namespace: test.cluster.Namespace, Shard: 0, Member: test.member}
			template, _ := planWorkloadTemplate(t, test.cluster, test.object)
			if template == nil {
				t.Fatalf("plan has no workload %s", test.object)
			}
			for _, live := range []bool{false, true} {
				stage := StageCreate
				if live {
					stage = StageLive
				}
				tmpl := template.DeepCopy()
				podMeta, podSpec, templateMeta, templateSpec := stampAndAdmit(t, tmpl, nc, live)
				if err := ComparePodToStampedTemplate(nc, podMeta, podSpec, templateMeta, templateSpec, stage, false); err != nil {
					t.Fatalf("honest %s (live=%t) did not match: %v", test.name, live, err)
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
	nc := NormContext{Class: ClassSource, ClusterName: cluster.Name, Namespace: cluster.Namespace, Shard: 0, Member: 0}
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

var _ = client.Object(nil)
