package podfence

import (
	"context"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	testClusterUID   = types.UID("cluster-uid")
	testWorkloadNS   = "database"
	testClusterName  = "example"
	testMembersShard = 3
)

func testControllerIdentities() ControllerIdentities {
	return ControllerIdentities{
		Operator:              "system:serviceaccount:pgshard-system:pgshard-controller-manager",
		StatefulSetController: "system:serviceaccount:kube-system:statefulset-controller",
		ReplicaSetController:  "system:serviceaccount:kube-system:replicaset-controller",
		DeploymentController:  "system:serviceaccount:kube-system:deployment-controller",
	}
}

func workloadScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := testScheme(t)
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := autoscalingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func clusterOwnerReference() metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: pgshardv1alpha1.GroupVersion.String(),
		Kind:       "PgShardCluster",
		Name:       testClusterName,
		UID:        testClusterUID,
		Controller: &controller,
	}
}

func testWorkloadCluster() *pgshardv1alpha1.PgShardCluster {
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testWorkloadNS, UID: testClusterUID},
		Spec:       pgshardv1alpha1.PgShardClusterSpec{MembersPerShard: testMembersShard},
	}
}

func stampedTemplate(t *testing.T, class owned.PodClass, shard, member int32) corev1.PodTemplateSpec {
	t.Helper()
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				owned.ClusterLabel:   testClusterName,
				owned.ComponentLabel: string(class),
			},
			Annotations: map[string]string{owned.PostgreSQLPodClusterUIDAnnotation: string(testClusterUID)},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "workload", Image: "pgshard/example:dev"}}},
	}
	if _, err := owned.ApplyContractStamp(&template, class, string(testClusterUID), shard, member, 1); err != nil {
		t.Fatal(err)
	}
	return template
}

func stampedMemberStatefulSet(t *testing.T) *appsv1.StatefulSet {
	t.Helper()
	replicas := int32(1)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      owned.PostgreSQLMemberStatefulSetName(testClusterName, 0, 0),
			Namespace: testWorkloadNS,
			UID:       "sts-uid",
			Labels: map[string]string{
				owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "postgresql",
				owned.ClusterLabel: testClusterName, owned.ShardLabel: "0000", owned.MemberLabel: "0000",
			},
			OwnerReferences: []metav1.OwnerReference{clusterOwnerReference()},
		},
		Spec: appsv1.StatefulSetSpec{Replicas: &replicas, Template: stampedTemplate(t, owned.ClassSource, 0, 0)},
	}
}

func workloadRequest(t *testing.T, object any, resource, subresource, name, username string, operation admissionv1.Operation) admission.Request {
	t.Helper()
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation:   operation,
		SubResource: subresource,
		Resource:    metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: resource},
		Name:        name,
		Namespace:   testWorkloadNS,
		Object:      runtime.RawExtension{Raw: marshalObject(t, object)},
		UserInfo:    authenticationv1.UserInfo{Username: username},
	}}
}

func TestWorkloadIntegrityValidatorAuthenticatesMemberStatefulSets(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	build := func(objects ...client.Object) *WorkloadIntegrityValidator {
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		return NewWorkloadIntegrityValidator(reader, identities, scheme)
	}

	statefulSet := stampedMemberStatefulSet(t)
	name := statefulSet.Name

	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadRequest(t, statefulSet, "statefulsets", "", name, identities.Operator, admissionv1.Create)); !response.Allowed {
		t.Fatalf("operator-authored stamped StatefulSet denied: %#v", response.Result)
	}

	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadRequest(t, statefulSet, "statefulsets", "", name, identities.StatefulSetController, admissionv1.Create)); response.Allowed ||
		!strings.Contains(response.Result.Message, "authored by the pgshard operator") {
		t.Fatalf("non-operator StatefulSet author accepted: %#v", response.Result)
	}

	tampered := statefulSet.DeepCopy()
	tampered.Spec.Template.Annotations[owned.PodContractHashAnnotation] = strings.Repeat("0", 64)
	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadRequest(t, tampered, "statefulsets", "", name, identities.Operator, admissionv1.Create)); response.Allowed ||
		!strings.Contains(response.Result.Message, "does not recompute") {
		t.Fatalf("tampered contract stamp accepted: %#v", response.Result)
	}

	overscaled := statefulSet.DeepCopy()
	two := int32(2)
	overscaled.Spec.Replicas = &two
	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadRequest(t, overscaled, "statefulsets", "", name, identities.Operator, admissionv1.Create)); response.Allowed ||
		!strings.Contains(response.Result.Message, "exactly one replica") {
		t.Fatalf("multi-replica member StatefulSet accepted: %#v", response.Result)
	}

	unmanaged := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: testWorkloadNS}}
	if response := build().Handle(context.Background(),
		workloadRequest(t, unmanaged, "statefulsets", "", "other", "system:serviceaccount:tenant:someone", admissionv1.Create)); response.Allowed ||
		!strings.Contains(response.Result.Message, "authored by the pgshard operator") {
		t.Fatalf("non-operator StatefulSet accepted in a fenced namespace: %#v", response.Result)
	}
	if response := build().Handle(context.Background(),
		workloadRequest(t, unmanaged, "statefulsets", "", "other", identities.Operator, admissionv1.Create)); !response.Allowed {
		t.Fatalf("operator-authored label-free StatefulSet denied: %#v", response.Result)
	}
}

func TestWorkloadIntegrityValidatorBoundsMemberScale(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	statefulSet := stampedMemberStatefulSet(t)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), statefulSet).Build()
	validator := NewWorkloadIntegrityValidator(reader, identities, scheme)

	within := &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: 1}}
	if response := validator.Handle(context.Background(),
		workloadRequest(t, within, "statefulsets", "scale", statefulSet.Name, identities.Operator, admissionv1.Update)); !response.Allowed {
		t.Fatalf("single-replica member scale denied: %#v", response.Result)
	}

	beyond := &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: 2}}
	if response := validator.Handle(context.Background(),
		workloadRequest(t, beyond, "statefulsets", "scale", statefulSet.Name, identities.Operator, admissionv1.Update)); response.Allowed ||
		!strings.Contains(response.Result.Message, "single replica") {
		t.Fatalf("member scale beyond one accepted: %#v", response.Result)
	}

	supporting := &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: 9}}
	if response := validator.Handle(context.Background(),
		workloadRequest(t, supporting, "deployments", "scale", "example-pooler", identities.Operator, admissionv1.Update)); !response.Allowed {
		t.Fatalf("deferred supporting scale denied: %#v", response.Result)
	}
}

func TestWorkloadIntegrityValidatorAuthenticatesSupportingReplicaSets(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	controller := true

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-orchestrator", Namespace: testWorkloadNS, UID: "deploy-uid",
			Labels: map[string]string{
				owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "orchestrator", owned.ClusterLabel: testClusterName,
			},
			OwnerReferences: []metav1.OwnerReference{clusterOwnerReference()},
		},
		Spec: appsv1.DeploymentSpec{Template: stampedTemplate(t, owned.ClassOrchestrator, 0, 0)},
	}
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-orchestrator-77abcde", Namespace: testWorkloadNS, UID: "rs-uid",
			Labels: map[string]string{
				owned.ComponentLabel: "orchestrator", owned.ClusterLabel: testClusterName, podTemplateHashLabel: "77abcde",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{Template: stampedTemplate(t, owned.ClassOrchestrator, 0, 0)},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), deployment).Build()
	validator := NewWorkloadIntegrityValidator(reader, identities, scheme)

	if response := validator.Handle(context.Background(),
		workloadRequest(t, replicaSet, "replicasets", "", replicaSet.Name, identities.DeploymentController, admissionv1.Create)); !response.Allowed {
		t.Fatalf("deployment-controller-authored supporting ReplicaSet denied: %#v", response.Result)
	}

	if response := validator.Handle(context.Background(),
		workloadRequest(t, replicaSet, "replicasets", "", replicaSet.Name, identities.Operator, admissionv1.Create)); response.Allowed ||
		!strings.Contains(response.Result.Message, "authored by the Deployment controller") {
		t.Fatalf("non-deployment-controller ReplicaSet author accepted: %#v", response.Result)
	}
}

func TestPodContractValidatorGatesOnReconcilerStamp(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	attempt := func(pod *corev1.Pod, username string, objects ...client.Object) admission.Response {
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Name: pod.Name, Namespace: pod.Namespace, Operation: admissionv1.Create,
			Object:   runtime.RawExtension{Raw: marshalObject(t, pod)},
			UserInfo: authenticationv1.UserInfo{Username: username},
		}}
		return NewPodCreateValidator(reader, identities, scheme).Handle(context.Background(), request)
	}

	supportingPod := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "example-orchestrator-77abcde-xyz", Namespace: testWorkloadNS,
				Labels: map[string]string{owned.ClusterLabel: testClusterName, owned.ComponentLabel: "orchestrator"},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "orchestrator"}}},
		}
	}

	stampless := supportingPod()
	if response := attempt(stampless, identities.ReplicaSetController); !response.Allowed {
		t.Fatalf("stampless supporting pod denied before activation: %#v", response.Result)
	}

	stamped := supportingPod()
	stamped.Annotations = map[string]string{
		owned.PostgreSQLPodClusterUIDAnnotation: string(testClusterUID),
		owned.PodContractHashAnnotation:         strings.Repeat("a", 64),
	}
	cluster := testWorkloadCluster()

	if response := attempt(stamped, "", cluster); response.Allowed ||
		!strings.Contains(response.Result.Message, "ReplicaSet controller") {
		t.Fatalf("stamped supporting pod bypassed creator identity: %#v", response.Result)
	}

	if response := attempt(stamped, identities.ReplicaSetController, cluster); response.Allowed ||
		!strings.Contains(response.Result.Message, "not owned by a ReplicaSet") {
		t.Fatalf("stamped supporting pod without a ReplicaSet parent accepted: %#v", response.Result)
	}
}

func workloadUpdateRequest(t *testing.T, old, updated any, resource, name, username string) admission.Request {
	t.Helper()
	request := workloadRequest(t, updated, resource, "", name, username, admissionv1.Update)
	request.OldObject = runtime.RawExtension{Raw: marshalObject(t, old)}
	return request
}

func orchestratorDeployment(t *testing.T) *appsv1.Deployment {
	t.Helper()
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-orchestrator", Namespace: testWorkloadNS, UID: "deploy-uid",
			Labels: map[string]string{
				owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "orchestrator", owned.ClusterLabel: testClusterName,
			},
			OwnerReferences: []metav1.OwnerReference{clusterOwnerReference()},
		},
		Spec: appsv1.DeploymentSpec{Template: stampedTemplate(t, owned.ClassOrchestrator, 0, 0)},
	}
}

func TestWorkloadIntegrityValidatorDeniesRogueReplicaSetLaundering(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	controller := true
	deployment := orchestratorDeployment(t)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), deployment).Build()
	validator := NewWorkloadIntegrityValidator(reader, identities, scheme)

	attackerTemplate := func() corev1.PodTemplateSpec {
		template := corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				owned.ClusterLabel: testClusterName, owned.ComponentLabel: "orchestrator",
			}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "workload", Image: "attacker/backdoor:latest"}}},
		}
		// The HMAC key contains only public domain inputs, so an attacker CAN
		// recompute a self-consistent stamp; provenance is the authenticator.
		if _, err := owned.ApplyContractStamp(&template, owned.ClassOrchestrator, string(testClusterUID), 0, 0, 1); err != nil {
			t.Fatal(err)
		}
		return template
	}
	rogue := func(template corev1.PodTemplateSpec) *appsv1.ReplicaSet {
		return &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: "innocuous", Namespace: testWorkloadNS,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}},
			},
			Spec: appsv1.ReplicaSetSpec{Template: template},
		}
	}

	if response := validator.Handle(context.Background(),
		workloadRequest(t, rogue(attackerTemplate()), "replicasets", "", "innocuous", "system:serviceaccount:tenant:attacker", admissionv1.Create)); response.Allowed ||
		!strings.Contains(response.Result.Message, "authored by the Deployment controller") {
		t.Fatalf("attacker-created label-free rogue ReplicaSet accepted: %#v", response.Result)
	}

	if response := validator.Handle(context.Background(),
		workloadRequest(t, rogue(attackerTemplate()), "replicasets", "", "innocuous", identities.DeploymentController, admissionv1.Create)); response.Allowed ||
		!strings.Contains(response.Result.Message, "does not match its owning Deployment") {
		t.Fatalf("self-stamped rogue ReplicaSet template accepted: %#v", response.Result)
	}

	laundered := attackerTemplate()
	laundered.Annotations = map[string]string{
		owned.PodContractHashAnnotation:       deployment.Spec.Template.Annotations[owned.PodContractHashAnnotation],
		owned.PodSecurityGenerationAnnotation: deployment.Spec.Template.Annotations[owned.PodSecurityGenerationAnnotation],
	}
	if response := validator.Handle(context.Background(),
		workloadRequest(t, rogue(laundered), "replicasets", "", "innocuous", identities.DeploymentController, admissionv1.Create)); response.Allowed ||
		!strings.Contains(response.Result.Message, "diverges from its stamped owning Deployment") {
		t.Fatalf("annotation-copying rogue ReplicaSet template accepted: %#v", response.Result)
	}
}

func TestWorkloadIntegrityValidatorDeniesIdentityTransitions(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	build := func(objects ...client.Object) *WorkloadIntegrityValidator {
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		return NewWorkloadIntegrityValidator(reader, identities, scheme)
	}

	protected := stampedMemberStatefulSet(t)
	stripped := protected.DeepCopy()
	delete(stripped.Labels, owned.ComponentLabel)
	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadUpdateRequest(t, protected, stripped, "statefulsets", protected.Name, identities.Operator)); response.Allowed ||
		!strings.Contains(response.Result.Message, "transition into or out of managed identity") {
		t.Fatalf("label removal from a protected StatefulSet accepted: %#v", response.Result)
	}

	reshard := protected.DeepCopy()
	reshard.Labels[owned.ShardLabel] = "0001"
	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadUpdateRequest(t, protected, reshard, "statefulsets", protected.Name, identities.Operator)); response.Allowed ||
		!strings.Contains(response.Result.Message, "identity is immutable") {
		t.Fatalf("shard identity mutation on a protected StatefulSet accepted: %#v", response.Result)
	}

	plain := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: testWorkloadNS, UID: "plain-uid"}}
	promoted := plain.DeepCopy()
	promoted.Labels = map[string]string{owned.ClusterLabel: testClusterName, owned.ComponentLabel: "pooler"}
	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadUpdateRequest(t, plain, promoted, "deployments", "plain", identities.Operator)); response.Allowed ||
		!strings.Contains(response.Result.Message, "transition into or out of managed identity") {
		t.Fatalf("promotion of an unmanaged Deployment into managed identity accepted: %#v", response.Result)
	}

	deployment := orchestratorDeployment(t)
	mutated := deployment.DeepCopy()
	mutated.Spec.Template.Spec.Containers[0].Image = "attacker/backdoor:latest"
	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadUpdateRequest(t, deployment, mutated, "deployments", deployment.Name, identities.DeploymentController)); response.Allowed ||
		!strings.Contains(response.Result.Message, "may not mutate") {
		t.Fatalf("Deployment-controller template mutation accepted: %#v", response.Result)
	}

	revisioned := deployment.DeepCopy()
	revisioned.Annotations = map[string]string{"deployment.kubernetes.io/revision": "2"}
	if response := build(testWorkloadCluster()).Handle(context.Background(),
		workloadUpdateRequest(t, deployment, revisioned, "deployments", deployment.Name, identities.DeploymentController)); !response.Allowed {
		t.Fatalf("Deployment-controller revision annotation sync denied: %#v", response.Result)
	}

	controller := true
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-orchestrator-77abcde", Namespace: testWorkloadNS, UID: "rs-uid",
			Labels: map[string]string{
				owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "orchestrator",
				owned.ClusterLabel: testClusterName, podTemplateHashLabel: "77abcde",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{Template: *deployment.Spec.Template.DeepCopy()},
	}
	scaled := replicaSet.DeepCopy()
	replicas := int32(3)
	scaled.Spec.Replicas = &replicas
	if response := build().Handle(context.Background(),
		workloadUpdateRequest(t, replicaSet, scaled, "replicasets", replicaSet.Name, identities.DeploymentController)); !response.Allowed {
		t.Fatalf("Deployment-controller ReplicaSet scaling denied: %#v", response.Result)
	}
	retemplated := replicaSet.DeepCopy()
	retemplated.Spec.Template.Spec.Containers[0].Image = "attacker/backdoor:latest"
	if response := build().Handle(context.Background(),
		workloadUpdateRequest(t, replicaSet, retemplated, "replicasets", replicaSet.Name, identities.DeploymentController)); response.Allowed ||
		!strings.Contains(response.Result.Message, "pod template is immutable") {
		t.Fatalf("ReplicaSet template mutation accepted: %#v", response.Result)
	}
}

func TestResolveStampedParentRequiresLiveRevisionEvidence(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	cluster := testWorkloadCluster()
	statefulSet := stampedMemberStatefulSet(t)
	statefulSet.Status.CurrentRevision = "rev-a"
	statefulSet.Status.UpdateRevision = "rev-b"
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, statefulSet).Build()

	memberPod := func(revision string) *corev1.Pod {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: statefulSet.Name + "-0", Namespace: testWorkloadNS}}
		if revision != "" {
			pod.Labels = map[string]string{controllerRevisionHashLabel: revision}
		}
		return pod
	}

	if _, _, _, response := resolveStampedParent(context.Background(), reader, testWorkloadNS, memberPod("rev-x"), contractPodMember, 0, 0, testClusterName, cluster); response == nil ||
		!strings.Contains(response.Result.Message, "does not match the live StatefulSet revision state") {
		t.Fatalf("forged member revision accepted: %#v", response)
	}
	if _, _, _, response := resolveStampedParent(context.Background(), reader, testWorkloadNS, memberPod(""), contractPodMember, 0, 0, testClusterName, cluster); response == nil ||
		!strings.Contains(response.Result.Message, "no controller revision evidence") {
		t.Fatalf("revision-free member pod accepted: %#v", response)
	}
	_, _, provenance, response := resolveStampedParent(context.Background(), reader, testWorkloadNS, memberPod("rev-b"), contractPodMember, 0, 0, testClusterName, cluster)
	if response != nil {
		t.Fatalf("live update-revision member pod denied: %#v", response.Result)
	}
	if provenance.ControllerRevisionHash != "rev-b" {
		t.Fatalf("member revision evidence = %q, want rev-b", provenance.ControllerRevisionHash)
	}

	blankStatus := stampedMemberStatefulSet(t)
	blankReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, blankStatus).Build()
	if _, _, _, response := resolveStampedParent(context.Background(), blankReader, testWorkloadNS, memberPod("rev-b"), contractPodMember, 0, 0, testClusterName, cluster); response == nil ||
		!strings.Contains(response.Result.Message, "does not match the live StatefulSet revision state") {
		t.Fatalf("member pod accepted against a StatefulSet with no recorded revisions: %#v", response)
	}

	controller := true
	deployment := orchestratorDeployment(t)
	hashlessReplicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-orchestrator-77abcde", Namespace: testWorkloadNS, UID: "rs-uid",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{Template: *deployment.Spec.Template.DeepCopy()},
	}
	supportingPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "example-orchestrator-77abcde-xyz", Namespace: testWorkloadNS,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: hashlessReplicaSet.Name, UID: hashlessReplicaSet.UID, Controller: &controller}},
	}}
	supportingReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, deployment, hashlessReplicaSet).Build()
	if _, _, _, response := resolveStampedParent(context.Background(), supportingReader, testWorkloadNS, supportingPod, contractPodOrchestrator, 0, 0, testClusterName, cluster); response == nil ||
		!strings.Contains(response.Result.Message, "no pod-template-hash evidence") {
		t.Fatalf("supporting pod accepted against a hash-free ReplicaSet: %#v", response)
	}
}

func TestPodContractValidatorCrossChecksSecurityGeneration(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	controller := true
	deployment := orchestratorDeployment(t)
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-orchestrator-77abcde", Namespace: testWorkloadNS, UID: "rs-uid",
			Labels:          map[string]string{podTemplateHashLabel: "77abcde"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{Template: *deployment.Spec.Template.DeepCopy()},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), deployment, replicaSet).Build()
	attempt := func(pod *corev1.Pod) admission.Response {
		request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Name: pod.Name, Namespace: testWorkloadNS, Operation: admissionv1.Create,
			Object:   runtime.RawExtension{Raw: marshalObject(t, pod)},
			UserInfo: authenticationv1.UserInfo{Username: identities.ReplicaSetController},
		}}
		return NewPodCreateValidator(reader, identities, scheme).Handle(context.Background(), request)
	}
	stampedPod := func(generation string) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "example-orchestrator-77abcde-xyz", Namespace: testWorkloadNS,
				Labels: map[string]string{owned.ClusterLabel: testClusterName, owned.ComponentLabel: "orchestrator"},
				Annotations: map[string]string{
					owned.PostgreSQLPodClusterUIDAnnotation: string(testClusterUID),
					owned.PodContractHashAnnotation:         deployment.Spec.Template.Annotations[owned.PodContractHashAnnotation],
				},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSet.Name, UID: replicaSet.UID, Controller: &controller}},
			},
			Spec: *deployment.Spec.Template.Spec.DeepCopy(),
		}
		if generation != "" {
			pod.Annotations[owned.PodSecurityGenerationAnnotation] = generation
		}
		return pod
	}

	if response := attempt(stampedPod("2")); response.Allowed ||
		!strings.Contains(response.Result.Message, "security generation does not match") {
		t.Fatalf("generation-skewed managed pod accepted: %#v", response.Result)
	}
	if response := attempt(stampedPod("")); response.Allowed ||
		!strings.Contains(response.Result.Message, "security generation does not match") {
		t.Fatalf("generation-free managed pod accepted: %#v", response.Result)
	}
}

func TestCanonicalSecurityGeneration(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]bool{
		"1": true, "42": true, "9223372036854775807": true,
		"": false, "0": false, "-1": false, "+1": false, "01": false, " 1": false, "1 ": false, "x": false,
	} {
		if _, ok := canonicalSecurityGeneration(raw); ok != want {
			t.Fatalf("canonicalSecurityGeneration(%q) = %v, want %v", raw, ok, want)
		}
	}
}

func poolerDeploymentFixture(t *testing.T) *appsv1.Deployment {
	t.Helper()
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-pooler", Namespace: testWorkloadNS, UID: "pooler-deploy-uid",
			Labels: map[string]string{
				owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "pooler", owned.ClusterLabel: testClusterName,
			},
			OwnerReferences: []metav1.OwnerReference{clusterOwnerReference()},
		},
		Spec: appsv1.DeploymentSpec{Template: stampedTemplate(t, owned.ClassPooler, 0, 0)},
	}
}

func testTopologyNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-a", UID: "node-uid-a",
			Labels: map[string]string{corev1.LabelTopologyZone: "zone-a", corev1.LabelTopologyRegion: "region-a"},
		},
		Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: "boot-a"}},
	}
}

func TestValidateBoundPodContractProjectsNodeEvidence(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	controller := true
	deployment := poolerDeploymentFixture(t)
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-pooler-77abcde", Namespace: testWorkloadNS, UID: "pooler-rs-uid",
			Labels:          map[string]string{podTemplateHashLabel: "77abcde"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{Template: *deployment.Spec.Template.DeepCopy()},
	}
	cluster := testWorkloadCluster()
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, deployment, replicaSet).Build()
	node := testTopologyNode()

	prebind := func() *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "example-pooler-77abcde-xyz", Namespace: testWorkloadNS,
				Labels: map[string]string{
					owned.ClusterLabel: testClusterName, owned.ComponentLabel: "pooler", podTemplateHashLabel: "77abcde",
				},
				Annotations:     map[string]string{},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSet.Name, UID: replicaSet.UID, Controller: &controller}},
			},
			Spec: *deployment.Spec.Template.Spec.DeepCopy(),
		}
		for key, value := range deployment.Spec.Template.Annotations {
			pod.Annotations[key] = value
		}
		return pod
	}

	if response := validateBoundPodContract(context.Background(), reader, prebind(), node, cluster); response != nil {
		t.Fatalf("honest pre-bind pod rejected at bind: %#v", response.Result)
	}

	stampless := prebind()
	delete(stampless.Annotations, owned.PodContractHashAnnotation)
	if response := validateBoundPodContract(context.Background(), reader, stampless, node, cluster); response != nil {
		t.Fatalf("stampless pod rejected before activation: %#v", response.Result)
	}

	forgedResidue := prebind()
	forgedResidue.Annotations[NodeUIDAnnotation] = "forged-node"
	if response := validateBoundPodContract(context.Background(), reader, forgedResidue, node, cluster); response == nil ||
		!strings.Contains(response.Result.Message, "node identity residue before it is bound") {
		t.Fatalf("pre-bind node residue accepted: %#v", response)
	}

	forgedTopology := prebind()
	forgedTopology.Labels[corev1.LabelTopologyZone] = "attacker-zone"
	if response := validateBoundPodContract(context.Background(), reader, forgedTopology, node, cluster); response == nil ||
		!strings.Contains(response.Result.Message, "topology label before it is bound") {
		t.Fatalf("pre-bind topology label accepted: %#v", response)
	}

	drift := prebind()
	drift.Spec.Containers[0].Image = "attacker/backdoor:latest"
	if response := validateBoundPodContract(context.Background(), reader, drift, node, cluster); response == nil ||
		!strings.Contains(response.Result.Message, "does not match its stamped contract") {
		t.Fatalf("drifted bound pod accepted: %#v", response)
	}
}

func TestMetadataValidatorDeniesAdoptionAndEscape(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	handler := NewMetadataValidator(testCodec(), scheme)

	// ADOPTION: unmanaged pod mutated into a managed identity.
	unmanaged := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: testWorkloadNS}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}}}
	adopted := unmanaged.DeepCopy()
	adopted.Labels = map[string]string{
		owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "postgresql",
		owned.ClusterLabel: testClusterName, owned.ShardLabel: "0000", owned.MemberLabel: "0000",
	}
	if response := handler.Handle(context.Background(), updateRequest(t, unmanaged, adopted, "")); response.Allowed ||
		!strings.Contains(response.Result.Message, "may not be mutated into a managed identity") {
		t.Fatalf("adoption of an unmanaged pod accepted: %#v", response.Result)
	}

	// ESCAPE: a managed member sheds its identity labels.
	member := managedPod()
	member.Spec.NodeName = ""
	delete(member.Annotations, NodeUIDAnnotation)
	delete(member.Annotations, NodeBootIDAnnotation)
	member.DeletionTimestamp = nil
	escaped := member.DeepCopy()
	delete(escaped.Labels, owned.ComponentLabel)
	if response := handler.Handle(context.Background(), updateRequest(t, member, escaped, "")); response.Allowed ||
		!strings.Contains(response.Result.Message, "immutable") {
		t.Fatalf("member identity escape accepted: %#v", response.Result)
	}
}

func TestMetadataValidatorProtectsSupportingPods(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	handler := NewMetadataValidator(testCodec(), scheme)

	supporting := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "example-orchestrator-77abcde-xyz", Namespace: testWorkloadNS,
				Labels: map[string]string{owned.ClusterLabel: testClusterName, owned.ComponentLabel: "orchestrator", podTemplateHashLabel: "77abcde"},
				Annotations: map[string]string{
					owned.PodContractHashAnnotation:       strings.Repeat("a", 64),
					owned.PodSecurityGenerationAnnotation: "1",
				},
			},
			Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "orchestrator", Image: "pgshard/orchestrator:dev"}}},
		}
	}

	if response := handler.Handle(context.Background(), updateRequest(t, supporting(), supporting(), "")); !response.Allowed {
		t.Fatalf("honest no-op supporting update denied: %#v", response.Result)
	}

	stampMutation := supporting()
	stampMutation.Annotations[owned.PodContractHashAnnotation] = strings.Repeat("b", 64)
	if response := handler.Handle(context.Background(), updateRequest(t, supporting(), stampMutation, "")); response.Allowed ||
		!strings.Contains(response.Result.Message, "identity is immutable") {
		t.Fatalf("supporting stamp mutation accepted: %#v", response.Result)
	}

	ephemeral := supporting()
	ephemeral.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "debug"}}}
	if response := handler.Handle(context.Background(), updateRequest(t, supporting(), ephemeral, "ephemeralcontainers")); response.Allowed ||
		!strings.Contains(response.Result.Message, "must not carry ephemeral containers") {
		t.Fatalf("supporting ephemeral container accepted: %#v", response.Result)
	}

	resized := supporting()
	resized.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
	if response := handler.Handle(context.Background(), updateRequest(t, supporting(), resized, "resize")); response.Allowed ||
		!strings.Contains(response.Result.Message, "spec is immutable") {
		t.Fatalf("supporting diverging resize accepted: %#v", response.Result)
	}

	escape := supporting()
	delete(escape.Labels, owned.ComponentLabel)
	if response := handler.Handle(context.Background(), updateRequest(t, supporting(), escape, "")); response.Allowed ||
		!strings.Contains(response.Result.Message, "shed its managed identity") {
		t.Fatalf("supporting identity escape accepted: %#v", response.Result)
	}
}

func TestPodCreateValidatorAdmitsHonestSupportingPod(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	controller := true
	deployment := poolerDeploymentFixture(t)
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-pooler-77abcde", Namespace: testWorkloadNS, UID: "pooler-rs-uid",
			Labels:          map[string]string{podTemplateHashLabel: "77abcde"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{Template: *deployment.Spec.Template.DeepCopy()},
	}
	cluster := testWorkloadCluster()
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, deployment, replicaSet).Build()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-pooler-77abcde-xyz", Namespace: testWorkloadNS,
			Labels: map[string]string{
				owned.ClusterLabel: testClusterName, owned.ComponentLabel: "pooler", podTemplateHashLabel: "77abcde",
			},
			Annotations:     map[string]string{},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSet.Name, UID: replicaSet.UID, Controller: &controller}},
		},
		Spec: *deployment.Spec.Template.Spec.DeepCopy(),
	}
	for key, value := range deployment.Spec.Template.Annotations {
		pod.Annotations[key] = value
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: pod.Name, Namespace: testWorkloadNS, Operation: admissionv1.Create,
		Object:   runtime.RawExtension{Raw: marshalObject(t, pod)},
		UserInfo: authenticationv1.UserInfo{Username: identities.ReplicaSetController},
	}}
	if response := NewPodCreateValidator(reader, identities, scheme).Handle(context.Background(), request); !response.Allowed {
		t.Fatalf("honest stamped supporting pod CREATE denied: %#v", response.Result)
	}
}

func TestMetadataValidatorHoldsSupportingContractImmutable(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	handler := NewMetadataValidator(testCodec(), scheme)
	controller := true
	base := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "example-orchestrator-77abcde-xyz", Namespace: testWorkloadNS,
				Labels: map[string]string{
					owned.ClusterLabel: testClusterName, owned.ComponentLabel: "orchestrator", podTemplateHashLabel: "77abcde",
					corev1.LabelTopologyZone: "zone-a",
				},
				Annotations: map[string]string{
					owned.PostgreSQLPodClusterUIDAnnotation: string(testClusterUID),
					owned.PodContractHashAnnotation:         strings.Repeat("a", 64),
					owned.PodSecurityGenerationAnnotation:   "1",
				},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "example-orchestrator-77abcde", UID: "rs-uid", Controller: &controller}},
			},
			Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "orchestrator", Image: "pgshard/orchestrator:dev"}}},
		}
	}

	for _, test := range []struct {
		name   string
		mutate func(*corev1.Pod)
		want   string
	}{
		{"pod-template-hash", func(pod *corev1.Pod) { pod.Labels[podTemplateHashLabel] = "beefbeef" }, "identity is immutable"},
		{"topology label", func(pod *corev1.Pod) { pod.Labels[corev1.LabelTopologyZone] = "attacker-zone" }, "identity is immutable"},
		{"extra finalizer", func(pod *corev1.Pod) { pod.Finalizers = append(pod.Finalizers, "attacker/hold") }, "finalizers are immutable"},
		{"extra owner reference", func(pod *corev1.Pod) {
			pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "sidecar", UID: "x"})
		}, "identity is immutable"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := base()
			test.mutate(changed)
			response := handler.Handle(context.Background(), updateRequest(t, base(), changed, ""))
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, test.want) {
				t.Fatalf("%s response = %#v", test.name, response)
			}
		})
	}

	if response := handler.Handle(context.Background(), updateRequest(t, base(), base(), "")); !response.Allowed {
		t.Fatalf("honest no-op supporting update denied: %#v", response.Result)
	}
}

func TestMetadataValidatorDeniesNoncanonicalManagedIdentity(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	handler := NewMetadataValidator(testCodec(), scheme)
	unmanaged := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: testWorkloadNS}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}}}
	// A managed-looking identity with a noncanonical (non-four-digit) shard would
	// read as managed to IsManagedPostgreSQLPod while dodging the canonical
	// classifier; it must be denied at UPDATE.
	malformed := unmanaged.DeepCopy()
	malformed.Labels = map[string]string{
		owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "postgresql",
		owned.ClusterLabel: testClusterName, owned.ShardLabel: "1", owned.RoleLabel: "primary", owned.MemberLabel: "0",
	}
	malformed.Annotations = map[string]string{owned.PostgreSQLPodClusterUIDAnnotation: string(testClusterUID)}
	malformed.Finalizers = []string{owned.PostgreSQLPodTerminationFinalizer}
	if response := handler.Handle(context.Background(), updateRequest(t, unmanaged, malformed, "")); response.Allowed ||
		!strings.Contains(response.Result.Message, "malformed identity") {
		t.Fatalf("noncanonical managed identity accepted: %#v", response.Result)
	}
}

func TestBindingValidatorRejectsSmuggledMetadata(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	pod := managedPod()
	pod.Spec.NodeName = ""
	pod.DeletionTimestamp = nil
	delete(pod.Annotations, NodeUIDAnnotation)
	delete(pod.Annotations, NodeBootIDAnnotation)
	node := testNode("node-a", "node-uid-a", "boot-a")
	cluster := managedClusterForPod(pod)
	cluster.Spec.MembersPerShard = 3
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, node, cluster).Build()

	base := func() *corev1.Binding {
		return &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
			Target:     corev1.ObjectReference{Kind: "Node", Name: node.Name},
		}
	}
	validate := func(binding *corev1.Binding) admission.Response {
		request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Name: pod.Name, Namespace: pod.Namespace, Operation: admissionv1.Create, SubResource: "binding",
			Object: runtime.RawExtension{Raw: marshalObject(t, binding)},
		}}
		return NewBindingValidator(reader, scheme).Handle(context.Background(), request)
	}

	if response := validate(base()); !response.Allowed {
		t.Fatalf("honest member binding denied: %#v", response.Result)
	}

	for _, test := range []struct {
		name   string
		mutate func(*corev1.Binding)
		want   string
	}{
		{"smuggled label", func(binding *corev1.Binding) { binding.Labels = map[string]string{"attacker": "x"} }, "unexpected label"},
		{"stamp override annotation", func(binding *corev1.Binding) {
			binding.Annotations = map[string]string{owned.PodContractHashAnnotation: "forged"}
		}, "unexpected annotation"},
		{"forged node incarnation", func(binding *corev1.Binding) {
			binding.Annotations = map[string]string{NodeUIDAnnotation: "forged-node"}
		}, "Node incarnation"},
		{"forged topology label", func(binding *corev1.Binding) {
			binding.Labels = map[string]string{corev1.LabelTopologyZone: "attacker-zone"}
		}, "topology label"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			binding := base()
			test.mutate(binding)
			response := validate(binding)
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, test.want) {
				t.Fatalf("%s response = %#v", test.name, response)
			}
		})
	}
}
