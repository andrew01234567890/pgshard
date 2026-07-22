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
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			owned.ClusterLabel:   testClusterName,
			owned.ComponentLabel: string(class),
		}},
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
		workloadRequest(t, unmanaged, "statefulsets", "", "other", "system:serviceaccount:tenant:someone", admissionv1.Create)); !response.Allowed {
		t.Fatalf("unmanaged StatefulSet denied: %#v", response.Result)
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
