package podfence

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	jsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestBindingAttestorPinsTheNodeIncarnationInTheBinding(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	pod := managedPod()
	pod.Spec.NodeName = ""
	pod.DeletionTimestamp = nil
	node := testNode("node-a", "node-uid-a", "boot-a")
	cluster := managedClusterForPod(pod)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, node, cluster).Build()
	handler := NewBindingAttestor(reader, scheme)
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID,
			Labels: map[string]string{
				owned.ManagedByLabel: "attacker", owned.ComponentLabel: "attacker", owned.ClusterLabel: "attacker",
				owned.ShardLabel: "attacker", owned.RoleLabel: "attacker", owned.MemberLabel: "attacker",
			},
			Annotations: map[string]string{owned.PostgreSQLPodClusterUIDAnnotation: "attacker"},
		},
		Target: corev1.ObjectReference{Kind: "Node", Name: node.Name},
	}
	raw := marshalObject(t, binding)
	response := handler.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: pod.Name, Namespace: pod.Namespace, Operation: admissionv1.Create, SubResource: "binding", Object: runtime.RawExtension{Raw: raw},
	}})
	if !response.Allowed {
		t.Fatalf("binding denied: %#v", response.Result)
	}
	patched := applyResponsePatch(t, raw, response)
	got := &corev1.Binding{}
	if err := json.Unmarshal(patched, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[NodeUIDAnnotation] != string(node.UID) || got.Annotations[NodeBootIDAnnotation] != node.Status.NodeInfo.BootID {
		t.Fatalf("binding identity = %#v", got.Annotations)
	}
	if got.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] != pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] {
		t.Fatalf("binding cluster identity = %#v", got.Annotations)
	}
	for _, key := range []string{owned.ManagedByLabel, owned.ComponentLabel, owned.ClusterLabel, owned.ShardLabel, owned.RoleLabel, owned.MemberLabel} {
		if got.Labels[key] != pod.Labels[key] {
			t.Fatalf("binding label %s = %q, want %q", key, got.Labels[key], pod.Labels[key])
		}
	}
	validationRequest := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: pod.Name, Namespace: pod.Namespace, Operation: admissionv1.Create, SubResource: "binding", Object: runtime.RawExtension{Raw: marshalObject(t, got)},
	}}
	validated := NewBindingValidator(reader, scheme).Handle(context.Background(), validationRequest)
	if !validated.Allowed {
		t.Fatalf("attested final binding denied: %#v", validated.Result)
	}

	conflicting := got.DeepCopy()
	conflicting.Labels[owned.ClusterLabel] = "rewritten-after-attestation"
	validationRequest.Object = runtime.RawExtension{Raw: marshalObject(t, conflicting)}
	validated = NewBindingValidator(reader, scheme).Handle(context.Background(), validationRequest)
	if validated.Allowed || validated.Result == nil || !strings.Contains(validated.Result.Message, "does not match") {
		t.Fatalf("post-mutation conflicting binding response = %#v", validated)
	}

	conflicting = got.DeepCopy()
	conflicting.Annotations[NodeBootIDAnnotation] = "replacement-boot"
	validationRequest.Object = runtime.RawExtension{Raw: marshalObject(t, conflicting)}
	validated = NewBindingValidator(reader, scheme).Handle(context.Background(), validationRequest)
	if validated.Allowed || validated.Result == nil || !strings.Contains(validated.Result.Message, "Node incarnation") {
		t.Fatalf("post-mutation conflicting Node identity response = %#v", validated)
	}
}

func TestBindingAdmissionAcceptsOnlyTheExactRoleNeutralBootstrapSource(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	pod := roleNeutralBootstrapSourcePod()
	pod.Spec.NodeName = ""
	pod.DeletionTimestamp = nil
	delete(pod.Annotations, NodeUIDAnnotation)
	delete(pod.Annotations, NodeBootIDAnnotation)
	if !IsManagedPostgreSQLPod(pod) {
		t.Fatalf("exact role-neutral bootstrap source is not managed: %#v", pod.ObjectMeta)
	}
	partialSource := pod.DeepCopy()
	partialSource.Spec.Containers[0].Env = append(partialSource.Spec.Containers[0].Env, corev1.EnvVar{Name: "PGSHARD_POSTGRES_GENERATION_DURABILITY", Value: "remote-apply-any-one"})
	if IsManagedPostgreSQLPod(partialSource) {
		t.Fatalf("partial generation upgrade was accepted as a managed bootstrap source: %#v", partialSource.Spec.Containers[0].Env)
	}
	standby := roleNeutralStandbyPod()
	if !IsManagedPostgreSQLPod(standby) {
		t.Fatalf("exact role-neutral physical standby is not managed: %#v", standby.ObjectMeta)
	}
	standby.Labels[owned.RoleLabel] = "replica"
	if owned.IsPostgreSQLReplicationStandbyPod(standby) {
		t.Fatalf("role-labeled physical standby bypassed its exact role-neutral classifier: %#v", standby.ObjectMeta)
	}
	generic := managedPod()
	delete(generic.Labels, owned.RoleLabel)
	if IsManagedPostgreSQLPod(generic) {
		t.Fatalf("generic roleless PostgreSQL Pod was accepted: %#v", generic.ObjectMeta)
	}

	node := testNode("node-a", "node-uid-a", "boot-a")
	cluster := managedClusterForPod(pod)
	cluster.Spec.MembersPerShard = 3
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, node, cluster).Build()
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID,
			Labels: map[string]string{owned.RoleLabel: "attacker"},
		},
		Target: corev1.ObjectReference{Kind: "Node", Name: node.Name},
	}
	raw := marshalObject(t, binding)
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: pod.Name, Namespace: pod.Namespace, Operation: admissionv1.Create, SubResource: "binding", Object: runtime.RawExtension{Raw: raw},
	}}
	attested := NewBindingAttestor(reader, scheme).Handle(context.Background(), request)
	if !attested.Allowed {
		t.Fatalf("role-neutral bootstrap-source binding denied: %#v", attested.Result)
	}
	got := &corev1.Binding{}
	if err := json.Unmarshal(applyResponsePatch(t, raw, attested), got); err != nil {
		t.Fatal(err)
	}
	if _, hasRole := got.Labels[owned.RoleLabel]; hasRole {
		t.Fatalf("attested role-neutral binding carries role label: %#v", got.Labels)
	}
	request.Object = runtime.RawExtension{Raw: marshalObject(t, got)}
	validated := NewBindingValidator(reader, scheme).Handle(context.Background(), request)
	if !validated.Allowed {
		t.Fatalf("attested role-neutral bootstrap-source binding denied: %#v", validated.Result)
	}
	changed := got.DeepCopy()
	changed.Labels[owned.RoleLabel] = ""
	request.Object = runtime.RawExtension{Raw: marshalObject(t, changed)}
	validated = NewBindingValidator(reader, scheme).Handle(context.Background(), request)
	if validated.Allowed || validated.Result == nil || !strings.Contains(validated.Result.Message, "does not match") {
		t.Fatalf("present-empty role binding response = %#v", validated)
	}
}

func TestBindingAdmissionDeniesNewLegacyBootstrapSourceButRetainsItsLifecycleFence(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	legacy := roleNeutralBootstrapSourcePod()
	legacy.Spec.NodeName = ""
	legacy.DeletionTimestamp = nil
	delete(legacy.Annotations, NodeUIDAnnotation)
	delete(legacy.Annotations, NodeBootIDAnnotation)
	delete(legacy.Annotations, owned.PostgreSQLGenerationDurabilityAnnotation)
	delete(legacy.Annotations, owned.PostgreSQLSynchronousStandbysAnnotation)
	legacy.Spec.Containers[0].Env = slices.DeleteFunc(legacy.Spec.Containers[0].Env, func(environment corev1.EnvVar) bool {
		return environment.Name == "PGSHARD_POSTGRES_GENERATION_DURABILITY" || environment.Name == "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES"
	})
	if !IsManagedPostgreSQLPod(legacy) {
		t.Fatal("complete v0.73 bootstrap source lost lifecycle fencing")
	}
	if owned.IsCurrentPostgreSQLReplicationBootstrapSourcePod(legacy) {
		t.Fatal("complete v0.73 bootstrap source was accepted as a current generation")
	}

	node := testNode("node-a", "node-uid-a", "boot-a")
	cluster := managedClusterForPod(legacy)
	cluster.Spec.MembersPerShard = 3
	cluster.Spec.Durability = pgshardv1alpha1.DurabilitySynchronous
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(legacy, node, cluster).Build()
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: legacy.Name, Namespace: legacy.Namespace, UID: legacy.UID},
		Target:     corev1.ObjectReference{Kind: "Node", Name: node.Name},
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: legacy.Name, Namespace: legacy.Namespace, Operation: admissionv1.Create, SubResource: "binding",
		Object: runtime.RawExtension{Raw: marshalObject(t, binding)},
	}}
	for name, handler := range map[string]admission.Handler{
		"attestor":  NewBindingAttestor(reader, scheme),
		"validator": NewBindingValidator(reader, scheme),
	} {
		response := handler.Handle(context.Background(), request)
		if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "legacy") {
			t.Fatalf("%s legacy binding response = %#v", name, response)
		}
	}

	boundLegacy := legacy.DeepCopy()
	boundLegacy.Spec.NodeName = "node-a"
	boundLegacy.Annotations[NodeUIDAnnotation] = "node-uid-a"
	boundLegacy.Annotations[NodeBootIDAnnotation] = "boot-a"
	changed := boundLegacy.DeepCopy()
	changed.Annotations[NodeUIDAnnotation] = "replacement"
	response := NewMetadataValidator(testCodec(), scheme).Handle(context.Background(), updateRequest(t, boundLegacy, changed, ""))
	if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "immutable") {
		t.Fatalf("legacy lifecycle identity mutation response = %#v", response)
	}
}

func TestBindingAdmissionRequiresTheLiveOwningCluster(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		cluster func(*corev1.Pod) *pgshardv1alpha1.PgShardCluster
		want    string
	}{
		{name: "missing", want: "no longer exists"},
		{name: "replacement UID", cluster: func(pod *corev1.Pod) *pgshardv1alpha1.PgShardCluster {
			cluster := managedClusterForPod(pod)
			cluster.UID = "replacement-cluster-uid"
			return cluster
		}, want: "live PgShardCluster UID"},
		{name: "deleting", cluster: func(pod *corev1.Pod) *pgshardv1alpha1.PgShardCluster {
			cluster := managedClusterForPod(pod)
			deleted := metav1.Now()
			cluster.DeletionTimestamp = &deleted
			cluster.Finalizers = []string{owned.ClusterResourceFinalizer}
			return cluster
		}, want: "PgShardCluster is deleting"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			scheme := testScheme(t)
			pod := managedPod()
			pod.Spec.NodeName = ""
			pod.DeletionTimestamp = nil
			node := testNode("node-a", "node-uid-a", "boot-a")
			objects := []client.Object{pod, node}
			if test.cluster != nil {
				objects = append(objects, test.cluster(pod))
			}
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			binding := &corev1.Binding{
				ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
				Target:     corev1.ObjectReference{Kind: "Node", Name: node.Name},
			}
			request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Name: pod.Name, Namespace: pod.Namespace, Operation: admissionv1.Create, SubResource: "binding", Object: runtime.RawExtension{Raw: marshalObject(t, binding)},
			}}
			for name, handler := range map[string]admission.Handler{
				"mutating":   NewBindingAttestor(reader, scheme),
				"validating": NewBindingValidator(reader, scheme),
			} {
				response := handler.Handle(context.Background(), request)
				if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, test.want) {
					t.Fatalf("%s binding response = %#v", name, response)
				}
			}
		})
	}
}

func TestBindingAdmissionRejectsPartiallyStrippedManagedPods(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	pod := managedPod()
	pod.Spec.NodeName = ""
	pod.DeletionTimestamp = nil
	pod.Finalizers = nil
	node := testNode("node-a", "node-uid-a", "boot-a")
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, node).Build()
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
		Target:     corev1.ObjectReference{Kind: "Node", Name: node.Name},
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: pod.Name, Namespace: pod.Namespace, Operation: admissionv1.Create, SubResource: "binding", Object: runtime.RawExtension{Raw: marshalObject(t, binding)},
	}}
	for name, handler := range map[string]admission.Handler{
		"mutating":   NewBindingAttestor(reader, scheme),
		"validating": NewBindingValidator(reader, scheme),
	} {
		t.Run(name, func(t *testing.T) {
			response := handler.Handle(context.Background(), request)
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "incomplete identity or no termination fence") {
				t.Fatalf("partially stripped Pod binding response = %#v", response)
			}
		})
	}
}

func TestBindingAdmissionAllowsNonPostgreSQLPgShardPods(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	for _, component := range []string{"orchestrator", "pooler"} {
		component := component
		t.Run(component, func(t *testing.T) {
			t.Parallel()
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "example-" + component, Namespace: "database", UID: types.UID("pod-uid-" + component),
				Labels: map[string]string{
					owned.ManagedByLabel: owned.ManagedByValue,
					owned.ComponentLabel: component,
					owned.ClusterLabel:   "example",
				},
			}}
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
			binding := &corev1.Binding{
				ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
				Target:     corev1.ObjectReference{Kind: "Node", Name: "node-a"},
			}
			request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Name: pod.Name, Namespace: pod.Namespace, Operation: admissionv1.Create, SubResource: "binding", Object: runtime.RawExtension{Raw: marshalObject(t, binding)},
			}}
			for name, handler := range map[string]admission.Handler{
				"mutating":   NewBindingAttestor(reader, scheme),
				"validating": NewBindingValidator(reader, scheme),
			} {
				response := handler.Handle(context.Background(), request)
				if !response.Allowed {
					t.Fatalf("%s binding denied for non-PostgreSQL pgshard Pod: %#v", name, response.Result)
				}
			}
		})
	}
}

func TestBindingAdmissionRejectsPostMutationPathConfusion(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	managed := managedPod()
	managed.Spec.NodeName = ""
	managed.DeletionTimestamp = nil
	unmanaged := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: managed.Name, Namespace: "redirected", UID: managed.UID}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(managed, unmanaged).Build()
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: managed.Name, Namespace: unmanaged.Namespace, UID: managed.UID},
		Target:     corev1.ObjectReference{Kind: "Node", Name: "node-a"},
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: managed.Name, Namespace: managed.Namespace, Operation: admissionv1.Create, SubResource: "binding",
		Object: runtime.RawExtension{Raw: marshalObject(t, binding)},
	}}
	for name, handler := range map[string]admission.Handler{
		"mutating":   NewBindingAttestor(reader, scheme),
		"validating": NewBindingValidator(reader, scheme),
	} {
		response := handler.Handle(context.Background(), request)
		if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "does not match the admission request path") {
			t.Fatalf("%s namespace-confused binding response = %#v", name, response)
		}
	}
}

func TestHandshakeAttestorAcknowledgesTheCurrentChallenge(t *testing.T) {
	t.Parallel()
	for _, members := range []int32{1, 3, 5} {
		members := members
		t.Run(fmt.Sprintf("members=%d", members), func(t *testing.T) {
			t.Parallel()
			scheme := testScheme(t)
			cluster := &pgshardv1alpha1.PgShardCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "example", Namespace: "database", UID: "cluster-uid",
					Annotations: map[string]string{HandshakeChallengeAnnotation: "challenge-a"},
				},
				Spec: pgshardv1alpha1.PgShardClusterSpec{MembersPerShard: members},
			}
			raw := marshalObject(t, cluster)
			codec := NewStaticHandshakeCodec([]byte("0123456789abcdef0123456789abcdef"))
			response := NewHandshakeAttestor(codec, scheme).Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Update, Object: runtime.RawExtension{Raw: raw},
			}})
			if !response.Allowed {
				t.Fatalf("fencing handshake denied: %#v", response.Result)
			}
			got := &pgshardv1alpha1.PgShardCluster{}
			if err := json.Unmarshal(applyResponsePatch(t, raw, response), got); err != nil {
				t.Fatal(err)
			}
			verified, err := codec.Verify(context.Background(), got)
			if err != nil {
				t.Fatal(err)
			}
			if !verified {
				t.Fatalf("fencing handshake receipt = %#v", got.Annotations)
			}
			replayed := got.DeepCopy()
			replayed.UID = "another-cluster-uid"
			verified, err = codec.Verify(context.Background(), replayed)
			if err != nil {
				t.Fatal(err)
			}
			if verified {
				t.Fatal("fencing handshake receipt was replayable across cluster UIDs")
			}
			missingChallenge := got.DeepCopy()
			delete(missingChallenge.Annotations, HandshakeChallengeAnnotation)
			verified, err = codec.Verify(context.Background(), missingChallenge)
			if err != nil {
				t.Fatalf("receipt-only handshake verification: %v", err)
			}
			if verified {
				t.Fatal("receipt-only handshake was authenticated without a challenge")
			}
		})
	}
}

func TestStatusAttestorAddsDurableKubeletProof(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	node := testNode("node-a", "node-uid-a", "boot-a")
	oldPod := managedPod()
	newPod := oldPod.DeepCopy()
	newPod.Status.Phase = corev1.PodFailed
	newPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "postgresql", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137}},
	}}
	handler := NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build(), testCodec(), scheme)
	request, raw := statusRequest(t, oldPod, newPod, "system:node:node-a", []string{"system:nodes"})
	response := handler.Handle(context.Background(), request)
	if !response.Allowed {
		t.Fatalf("kubelet terminal status denied: %#v", response.Result)
	}
	got := &corev1.Pod{}
	if err := json.Unmarshal(applyResponsePatch(t, raw, response), got); err != nil {
		t.Fatal(err)
	}
	if !HasTerminationAttestation(got) {
		t.Fatalf("terminal status has no valid attestation: %#v", got.Status.Conditions)
	}
}

func TestStatusAttestorCanAttestAnExistingTerminalPhase(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	node := testNode("node-a", "node-uid-a", "boot-a")
	oldPod := managedPod()
	oldPod.Status.Phase = corev1.PodFailed
	oldPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "postgresql", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137}},
	}}
	newPod := oldPod.DeepCopy()
	newPod.Status.Message = "kubelet retry"
	handler := NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build(), testCodec(), scheme)
	request, raw := statusRequest(t, oldPod, newPod, "system:node:node-a", []string{"system:nodes"})
	response := handler.Handle(context.Background(), request)
	if !response.Allowed {
		t.Fatalf("existing terminal status denied: %#v", response.Result)
	}
	got := &corev1.Pod{}
	if err := json.Unmarshal(applyResponsePatch(t, raw, response), got); err != nil {
		t.Fatal(err)
	}
	if !HasTerminationAttestation(got) {
		t.Fatalf("existing terminal status has no valid attestation: %#v", got.Status.Conditions)
	}
}

func TestStatusAttestorAcceptsNeverStartedWaitingContainers(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	node := testNode("node-a", "node-uid-a", "boot-a")
	oldPod := managedPod()
	oldPod.Spec.InitContainers = []corev1.Container{{Name: "bootstrap-postgresql"}}
	newPod := oldPod.DeepCopy()
	newPod.Status.Phase = corev1.PodFailed
	newPod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
		Name: "bootstrap-postgresql", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
	}}
	newPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "postgresql", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
	}}
	handler := NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build(), testCodec(), scheme)
	request, raw := statusRequest(t, oldPod, newPod, "system:node:node-a", []string{"system:nodes"})
	response := handler.Handle(context.Background(), request)
	if !response.Allowed {
		t.Fatalf("never-started terminal status denied: %#v", response.Result)
	}
	got := &corev1.Pod{}
	if err := json.Unmarshal(applyResponsePatch(t, raw, response), got); err != nil {
		t.Fatal(err)
	}
	if !HasTerminationAttestation(got) {
		t.Fatalf("never-started terminal status has no valid attestation: %#v", got.Status.Conditions)
	}
}

func TestStatusAttestorProtectsManagedMetadataOnTheStatusSubresource(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "managed-by label", mutate: func(pod *corev1.Pod) { delete(pod.Labels, owned.ManagedByLabel) }},
		{name: "component label", mutate: func(pod *corev1.Pod) { delete(pod.Labels, owned.ComponentLabel) }},
		{name: "cluster label", mutate: func(pod *corev1.Pod) { delete(pod.Labels, owned.ClusterLabel) }},
		{name: "shard label", mutate: func(pod *corev1.Pod) { delete(pod.Labels, owned.ShardLabel) }},
		{name: "role label", mutate: func(pod *corev1.Pod) { delete(pod.Labels, owned.RoleLabel) }},
		{name: "member label", mutate: func(pod *corev1.Pod) { delete(pod.Labels, owned.MemberLabel) }},
		{name: "cluster UID annotation", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, owned.PostgreSQLPodClusterUIDAnnotation) }},
		{name: "node UID annotation", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, NodeUIDAnnotation) }},
		{name: "node boot ID annotation", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, NodeBootIDAnnotation) }},
		{name: "termination finalizer", mutate: func(pod *corev1.Pod) { pod.Finalizers = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			oldPod := managedPod()
			newPod := oldPod.DeepCopy()
			test.mutate(newPod)
			request, _ := statusRequest(t, oldPod, newPod, "system:node:node-a", []string{"system:nodes"})
			response := NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).Build(), testCodec(), scheme).Handle(context.Background(), request)
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "identity changed") {
				t.Fatalf("protected metadata removal response = %#v", response)
			}
		})
	}
}

func TestStatusAttestorRejectsTerminalPhaseReversal(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	oldPod := managedPod()
	oldPod.Status.Phase = corev1.PodFailed
	oldPod.Status.Conditions = append(oldPod.Status.Conditions, validAttestation(oldPod))
	newPod := oldPod.DeepCopy()
	newPod.Status.Phase = corev1.PodRunning
	request, _ := statusRequest(t, oldPod, newPod, "system:node:node-a", []string{"system:nodes"})
	response := NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).Build(), testCodec(), scheme).Handle(context.Background(), request)
	if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "terminal phase is immutable") {
		t.Fatalf("terminal phase reversal response = %#v", response)
	}
}

func TestStatusValidatorAcceptsOnlyTheAttestorOutput(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	node := testNode("node-a", "node-uid-a", "boot-a")
	oldPod := managedPod()
	terminal := oldPod.DeepCopy()
	terminal.Status.Phase = corev1.PodFailed
	terminal.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "postgresql", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137}},
	}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
	request, raw := statusRequest(t, oldPod, terminal, "system:node:node-a", []string{"system:nodes"})
	mutated := NewStatusAttestor(reader, testCodec(), scheme).Handle(context.Background(), request)
	if !mutated.Allowed {
		t.Fatalf("authentic terminal status mutation denied: %#v", mutated.Result)
	}
	finalPod := &corev1.Pod{}
	if err := json.Unmarshal(applyResponsePatch(t, raw, mutated), finalPod); err != nil {
		t.Fatal(err)
	}
	request, _ = statusRequest(t, oldPod, finalPod, "system:node:node-a", []string{"system:nodes"})
	validated := NewStatusValidator(reader, testCodec(), scheme).Handle(context.Background(), request)
	if !validated.Allowed {
		t.Fatalf("authentic terminal status validation denied: %#v", validated.Result)
	}

	forged := terminal.DeepCopy()
	forged.Status.Conditions = append(forged.Status.Conditions, NewTerminationAttestation(forged, metav1.Now(), "v1.forged"))
	request, _ = statusRequest(t, oldPod, forged, "system:node:node-a", []string{"system:nodes"})
	validated = NewStatusValidator(reader, testCodec(), scheme).Handle(context.Background(), request)
	if validated.Allowed || validated.Result == nil || !strings.Contains(validated.Result.Message, "not authenticated") {
		t.Fatalf("post-mutation forged terminal receipt response = %#v", validated)
	}

	stripped := finalPod.DeepCopy()
	stripped.Finalizers = nil
	request, _ = statusRequest(t, oldPod, stripped, "system:node:node-a", []string{"system:nodes"})
	validated = NewStatusValidator(reader, testCodec(), scheme).Handle(context.Background(), request)
	if validated.Allowed || validated.Result == nil || !strings.Contains(validated.Result.Message, "identity changed during a status update") {
		t.Fatalf("post-mutation finalizer stripping response = %#v", validated)
	}
}

func TestStatusAttestorRejectsControlPlaneAndWrongNodeHistories(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	oldPod := managedPod()
	terminal := oldPod.DeepCopy()
	terminal.Status.Phase = corev1.PodFailed
	tests := []struct {
		name     string
		objects  []client.Object
		username string
		groups   []string
		want     string
	}{
		{
			name: "PodGC on live node", objects: []client.Object{testNode("node-a", "node-uid-a", "boot-a")},
			username: "system:kube-controller-manager", want: "not reported by the authenticated kubelet",
		},
		{
			name: "orphaned node", username: "system:node:node-a", groups: []string{"system:nodes"},
			want: "no longer exists",
		},
		{
			name: "same-name replacement", objects: []client.Object{testNode("node-a", "replacement-uid", "replacement-boot")},
			username: "system:node:node-a", groups: []string{"system:nodes"}, want: "not the Pod's binding-time node incarnation",
		},
		{
			name: "same Node object after reboot", objects: []client.Object{testNode("node-a", "node-uid-a", "replacement-boot")},
			username: "system:node:node-a", groups: []string{"system:nodes"}, want: "not the Pod's binding-time node incarnation",
		},
		{
			name: "wrong node identity", objects: []client.Object{testNode("node-a", "node-uid-a", "boot-a")},
			username: "system:node:node-b", groups: []string{"system:nodes"}, want: "not reported by the authenticated kubelet",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			handler := NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).WithObjects(test.objects...).Build(), testCodec(), scheme)
			request, _ := statusRequest(t, oldPod, terminal, test.username, test.groups)
			response := handler.Handle(context.Background(), request)
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, test.want) {
				t.Fatalf("response = %#v, want denial containing %q", response, test.want)
			}
		})
	}
}

func TestStatusAttestorRequiresCompleteStoppedContainerEvidence(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	node := testNode("node-a", "node-uid-a", "boot-a")
	oldPod := managedPod()
	tests := []struct {
		name     string
		statuses []corev1.ContainerStatus
		want     string
	}{
		{name: "missing status", want: "omits application container status"},
		{name: "running", statuses: []corev1.ContainerStatus{{Name: "postgresql", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}, want: "still reports application container postgresql running"},
		{name: "ambiguous", statuses: []corev1.ContainerStatus{{Name: "postgresql"}}, want: "ambiguous application container state"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			terminal := oldPod.DeepCopy()
			terminal.Status.Phase = corev1.PodFailed
			terminal.Status.ContainerStatuses = test.statuses
			handler := NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).WithObjects(node.DeepCopy()).Build(), testCodec(), scheme)
			request, _ := statusRequest(t, oldPod, terminal, "system:node:node-a", []string{"system:nodes"})
			response := handler.Handle(context.Background(), request)
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, test.want) {
				t.Fatalf("response = %#v, want denial containing %q", response, test.want)
			}
		})
	}
}

func TestPhaseAloneNeverReleasesTheMetadataFence(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	oldPod := managedPod()
	oldPod.Status.Phase = corev1.PodFailed
	newPod := oldPod.DeepCopy()
	newPod.Finalizers = nil
	handler := NewMetadataValidator(testCodec(), scheme)
	response := handler.Handle(context.Background(), updateRequest(t, oldPod, newPod, ""))
	if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "authenticated process-stop evidence") {
		t.Fatalf("phase-only finalizer removal response = %#v", response)
	}

	attested := oldPod.DeepCopy()
	attested.Status.Conditions = append(attested.Status.Conditions, validAttestation(attested))
	released := attested.DeepCopy()
	released.Finalizers = nil
	response = handler.Handle(context.Background(), updateRequest(t, attested, released, ""))
	if !response.Allowed {
		t.Fatalf("attested finalizer removal denied: %#v", response.Result)
	}
}

func TestMetadataValidatorProtectsTheBindingIdentity(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	oldPod := managedPod()
	changed := oldPod.DeepCopy()
	changed.Annotations[NodeUIDAnnotation] = "replacement"
	response := NewMetadataValidator(testCodec(), scheme).Handle(context.Background(), updateRequest(t, oldPod, changed, ""))
	if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "immutable") {
		t.Fatalf("binding identity mutation response = %#v", response)
	}
}

func TestMetadataValidatorProtectsAttestedPodGenerationAcrossSpecSubresources(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	oldPod := managedPod()
	oldPod.Status.Phase = corev1.PodFailed
	oldPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "postgresql", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
	}}
	oldPod.Status.Conditions = append(oldPod.Status.Conditions, validAttestation(oldPod))
	tests := []struct {
		name        string
		subresource string
		mutate      func(*corev1.Pod)
	}{
		{name: "main image update", mutate: func(pod *corev1.Pod) { pod.Spec.Containers[0].Image = "replacement" }},
		{name: "ephemeral container", subresource: "ephemeralcontainers", mutate: func(pod *corev1.Pod) {
			pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, corev1.EphemeralContainer{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "debug"}})
		}},
		{name: "in-place resize", subresource: "resize", mutate: func(pod *corev1.Pod) {
			pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := oldPod.DeepCopy()
			changed.Generation++
			test.mutate(changed)
			response := NewMetadataValidator(testCodec(), scheme).Handle(context.Background(), updateRequest(t, oldPod, changed, test.subresource))
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "spec and generation are immutable") {
				t.Fatalf("attested Pod %s mutation response = %#v", test.name, response)
			}
		})
	}
}

func TestUnscheduledDeletingPodCanReleaseItsFence(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	for _, test := range []struct {
		name string
		pod  func() *corev1.Pod
	}{
		{name: "serving member", pod: managedPod},
		{name: "role-neutral bootstrap source", pod: roleNeutralBootstrapSourcePod},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			oldPod := test.pod()
			oldPod.Spec.NodeName = ""
			delete(oldPod.Annotations, NodeUIDAnnotation)
			delete(oldPod.Annotations, NodeBootIDAnnotation)
			newPod := oldPod.DeepCopy()
			newPod.Finalizers = nil
			response := NewMetadataValidator(testCodec(), scheme).Handle(context.Background(), updateRequest(t, oldPod, newPod, ""))
			if !response.Allowed {
				t.Fatalf("unassigned Pod fence release denied: %#v", response.Result)
			}
		})
	}
}

func TestRoleNeutralBootstrapSourceIdentityIsImmutable(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	for _, mutation := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "present-empty role", mutate: func(pod *corev1.Pod) { pod.Labels[owned.RoleLabel] = "" }},
		{name: "missing runtime annotation", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, owned.PostgreSQLRuntimeAnnotation) }},
		{name: "empty runtime annotation", mutate: func(pod *corev1.Pod) { pod.Annotations[owned.PostgreSQLRuntimeAnnotation] = "" }},
		{name: "missing generation durability", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, owned.PostgreSQLGenerationDurabilityAnnotation) }},
		{name: "changed generation durability", mutate: func(pod *corev1.Pod) { pod.Annotations[owned.PostgreSQLGenerationDurabilityAnnotation] = "local" }},
		{name: "missing synchronous candidates", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, owned.PostgreSQLSynchronousStandbysAnnotation) }},
		{name: "changed synchronous candidates", mutate: func(pod *corev1.Pod) {
			pod.Annotations[owned.PostgreSQLSynchronousStandbysAnnotation] = "pgshard_member_0001"
		}},
	} {
		mutation := mutation
		for _, subresource := range []string{"", "status"} {
			subresource := subresource
			t.Run(mutation.name+"/"+subresource, func(t *testing.T) {
				t.Parallel()
				oldPod := roleNeutralBootstrapSourcePod()
				newPod := oldPod.DeepCopy()
				mutation.mutate(newPod)
				var response admission.Response
				if subresource == "status" {
					request, _ := statusRequest(t, oldPod, newPod, "system:node:node-a", []string{"system:nodes"})
					response = NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).Build(), testCodec(), scheme).Handle(context.Background(), request)
				} else {
					response = NewMetadataValidator(testCodec(), scheme).Handle(context.Background(), updateRequest(t, oldPod, newPod, ""))
				}
				if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "identity") {
					t.Fatalf("%s through %q response = %#v", mutation.name, subresource, response)
				}
			})
		}
	}
}

func TestGenerationAnnotationCannotBeAFirstStepToEscapeTheLifecycleFence(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	for _, firstSubresource := range []string{"", "status"} {
		firstSubresource := firstSubresource
		t.Run(firstSubresource, func(t *testing.T) {
			t.Parallel()
			oldPod := roleNeutralBootstrapSourcePod()
			firstStep := oldPod.DeepCopy()
			delete(firstStep.Annotations, owned.PostgreSQLGenerationDurabilityAnnotation)

			var firstResponse admission.Response
			if firstSubresource == "status" {
				request, _ := statusRequest(t, oldPod, firstStep, "system:node:node-a", []string{"system:nodes"})
				firstResponse = NewStatusAttestor(fake.NewClientBuilder().WithScheme(scheme).Build(), testCodec(), scheme).Handle(context.Background(), request)
			} else {
				firstResponse = NewMetadataValidator(testCodec(), scheme).Handle(context.Background(), updateRequest(t, oldPod, firstStep, ""))
			}
			if firstResponse.Allowed || firstResponse.Result == nil || !strings.Contains(firstResponse.Result.Message, "identity") {
				t.Fatalf("first annotation-stripping step through %q response = %#v", firstSubresource, firstResponse)
			}

			secondStep := firstStep.DeepCopy()
			secondStep.Finalizers = nil
			secondResponse := NewMetadataValidator(testCodec(), scheme).Handle(context.Background(), updateRequest(t, oldPod, secondStep, ""))
			if secondResponse.Allowed || secondResponse.Result == nil || !strings.Contains(secondResponse.Result.Message, "immutable") {
				t.Fatalf("two-step finalizer escape retry after rejected %q mutation response = %#v", firstSubresource, secondResponse)
			}
		})
	}
}

func TestNamespaceValidatorMakesTheFencingOptInStickyAcrossSubresources(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	oldNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "database", Labels: map[string]string{NamespaceLabel: NamespaceLabelValue},
	}}
	for _, subresource := range []string{"", "status", "finalize"} {
		t.Run(subresource, func(t *testing.T) {
			t.Parallel()
			newNamespace := oldNamespace.DeepCopy()
			delete(newNamespace.Labels, NamespaceLabel)
			response := NewNamespaceValidator(scheme).Handle(context.Background(), updateRequest(t, oldNamespace, newNamespace, subresource))
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "immutable") {
				t.Fatalf("fencing label removal through %q response = %#v", subresource, response)
			}
		})
	}
}

func managedPod() *corev1.Pod {
	deletion := metav1.NewTime(time.Unix(100, 0))
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-shard-0000-0", Namespace: "database", UID: types.UID("pod-uid"), Generation: 3,
			DeletionTimestamp: &deletion,
			Finalizers:        []string{owned.PostgreSQLPodTerminationFinalizer},
			Labels: map[string]string{
				owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "postgresql", owned.ClusterLabel: "example",
				owned.ShardLabel: "0000", owned.RoleLabel: "primary", owned.MemberLabel: "0000",
			},
			Annotations: map[string]string{
				owned.PostgreSQLPodClusterUIDAnnotation: "cluster-uid",
				NodeUIDAnnotation:                       "node-uid-a",
				NodeBootIDAnnotation:                    "boot-a",
			},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "postgresql"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func roleNeutralBootstrapSourcePod() *corev1.Pod {
	pod := managedPod()
	delete(pod.Labels, owned.RoleLabel)
	pod.Annotations[owned.PostgreSQLRuntimeAnnotation] = string(owned.PostgreSQLRuntimeAgentQuarantine)
	pod.Annotations[owned.PostgreSQLGenerationDurabilityAnnotation] = "remote-apply-any-one"
	pod.Annotations[owned.PostgreSQLSynchronousStandbysAnnotation] = "pgshard_member_0001,pgshard_member_0002"
	automount := false
	pod.Spec.AutomountServiceAccountToken = &automount
	pod.Spec.ServiceAccountName = owned.PostgreSQLAgentServiceAccountName(pod.Labels[owned.ClusterLabel], 0)
	pod.Spec.Containers = []corev1.Container{{
		Name: "postgresql",
		Env: []corev1.EnvVar{
			{Name: "PGSHARD_POSTGRES_MODE", Value: "replication-bootstrap-primary"},
			{Name: "PGSHARD_POSTGRES_HBA_FILE", Value: "/etc/pgshard/replication-bootstrap-primary.pg_hba.conf"},
			{Name: "PGSHARD_POSTGRES_GENERATION_DURABILITY", Value: "remote-apply-any-one"},
			{Name: "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES", Value: "pgshard_member_0001,pgshard_member_0002"},
		},
		Ports: []corev1.ContainerPort{{Name: "agent-http", ContainerPort: owned.HTTPPort, Protocol: corev1.ProtocolTCP}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "kubernetes-api", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount"},
			{Name: "runtime", MountPath: "/run/pgshard"},
		},
		StartupProbe:   &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}},
		LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz"}}},
	}}
	pod.Spec.Volumes = []corev1.Volume{{Name: "kubernetes-api", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}}}}
	return pod
}

func roleNeutralStandbyPod() *corev1.Pod {
	pod := managedPod()
	pod.Name = owned.PostgreSQLMemberStatefulSetName(pod.Labels[owned.ClusterLabel], 0, 1) + "-0"
	pod.Labels[owned.MemberLabel] = "0001"
	delete(pod.Labels, owned.RoleLabel)
	pod.Annotations[owned.PostgreSQLRuntimeAnnotation] = string(owned.PostgreSQLRuntimeAgentQuarantine)
	automount := false
	pod.Spec.AutomountServiceAccountToken = &automount
	pod.Spec.ServiceAccountName = owned.PostgreSQLStandbyServiceAccountName(pod.Labels[owned.ClusterLabel], 0)
	pod.Spec.Containers = []corev1.Container{{
		Name: "postgresql",
		Env: []corev1.EnvVar{
			{Name: "PGSHARD_CLUSTER_UID", Value: pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation]},
			{Name: "PGSHARD_POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
			{Name: "PGSHARD_POSTGRES_MODE", Value: "replication-standby"},
			{Name: "PGSHARD_POSTGRES_HBA_FILE", Value: "/etc/pgshard/quarantine.pg_hba.conf"},
			{Name: "PGSHARD_POSTGRES_PRIMARY_HOST", Value: "example-shard-0000-0.example-shard-0000.database.svc"},
			{Name: "PGSHARD_POSTGRES_PRIMARY_PORT", Value: "5432"},
			{Name: "PGSHARD_POSTGRES_PRIMARY_SLOT_NAME", Value: "pgshard_member_0001"},
			{Name: "PGSHARD_POSTGRES_PRIMARY_PASSFILE", Value: "/run/pgshard/standby-auth/passfile"},
		},
		Ports: []corev1.ContainerPort{{Name: "agent-http", ContainerPort: owned.HTTPPort, Protocol: corev1.ProtocolTCP}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "runtime", MountPath: "/run/pgshard"},
			{Name: "standby-passfile", MountPath: "/run/pgshard/standby-auth", ReadOnly: true},
		},
		StartupProbe:   &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}},
		LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz"}}},
	}}
	pod.Spec.Volumes = []corev1.Volume{
		{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
		{
			Name: "standby-passfile",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium: corev1.StorageMediumMemory,
			}},
		},
	}
	return pod
}

func managedClusterForPod(pod *corev1.Pod) *pgshardv1alpha1.PgShardCluster {
	return &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{
		Name:      pod.Labels[owned.ClusterLabel],
		Namespace: pod.Namespace,
		UID:       types.UID(pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation]),
	}}
}

func testNode(name string, uid types.UID, bootID string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
		Status:     corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: bootID}},
	}
}

func validAttestation(pod *corev1.Pod) corev1.PodCondition {
	receipt, err := testCodec().TerminationReceipt(context.Background(), pod)
	if err != nil {
		panic(err)
	}
	return NewTerminationAttestation(pod, metav1.Now(), receipt)
}

func testCodec() *HandshakeCodec {
	return NewStaticHandshakeCodec([]byte("0123456789abcdef0123456789abcdef"))
}

func statusRequest(t *testing.T, oldPod, newPod *corev1.Pod, username string, groups []string) (admission.Request, []byte) {
	t.Helper()
	raw := marshalObject(t, newPod)
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Update, SubResource: "status", Object: runtime.RawExtension{Raw: raw},
		OldObject: runtime.RawExtension{Raw: marshalObject(t, oldPod)},
		UserInfo:  authenticationv1.UserInfo{Username: username, Groups: groups},
	}}, raw
}

func updateRequest(t *testing.T, oldObject, newObject any, subresource string) admission.Request {
	t.Helper()
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Update, SubResource: subresource,
		Object: runtime.RawExtension{Raw: marshalObject(t, newObject)}, OldObject: runtime.RawExtension{Raw: marshalObject(t, oldObject)},
	}}
}

func marshalObject(t *testing.T, object any) []byte {
	t.Helper()
	raw, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func applyResponsePatch(t *testing.T, original []byte, response admission.Response) []byte {
	t.Helper()
	rawPatch, err := json.Marshal(response.Patches)
	if err != nil {
		t.Fatal(err)
	}
	patch, err := jsonpatch.DecodePatch(rawPatch)
	if err != nil {
		t.Fatal(err)
	}
	result, err := patch.Apply(original)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
