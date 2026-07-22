package podfence

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	sigsjson "sigs.k8s.io/json"
)

const (
	WorkloadWebhookName = "vpostgresqlworkload.pgshard.io"
	WorkloadWebhookPath = "/validate-apps-v1-postgresqlworkload"

	podTemplateHashLabel  = "pod-template-hash"
	componentPostgreSQL   = "postgresql"
	componentPooler       = "pooler"
	componentOrchestrator = "orchestrator"

	pgShardClusterKind = "PgShardCluster"
	replicaSetKind     = "ReplicaSet"
	deploymentKind     = "Deployment"
)

// ControllerIdentities are the authenticated usernames whose provenance the
// contract layer trusts. Every value is an unforgeable request.userInfo.Username
// asserted by the API server, never a self-declared label or annotation.
type ControllerIdentities struct {
	Operator              string
	StatefulSetController string
	ReplicaSetController  string
	DeploymentController  string
}

// contractPodKind is the protected class of an admitted pod, resolved from its
// own labels before any live-parent lookup.
type contractPodKind int

const (
	contractPodUnmanaged contractPodKind = iota
	contractPodMember
	contractPodPooler
	contractPodOrchestrator
)

// classifyContractPod resolves a pod's protected class from its labels. A member
// pod must carry a parseable shard and member; supporting pods are identified by
// their component and cluster labels. Anything else is unmanaged.
func classifyContractPod(pod *corev1.Pod) (contractPodKind, int32, int32, string) {
	clusterName := pod.Labels[owned.ClusterLabel]
	if clusterName == "" {
		return contractPodUnmanaged, 0, 0, ""
	}
	switch pod.Labels[owned.ComponentLabel] {
	case componentPostgreSQL:
		shard, shardOK := owned.ParseIdentityLabel(pod.Labels[owned.ShardLabel])
		member, memberOK := owned.ParseIdentityLabel(pod.Labels[owned.MemberLabel])
		if !shardOK || !memberOK {
			return contractPodUnmanaged, 0, 0, ""
		}
		return contractPodMember, shard, member, clusterName
	case componentPooler:
		return contractPodPooler, 0, 0, clusterName
	case componentOrchestrator:
		return contractPodOrchestrator, 0, 0, clusterName
	}
	return contractPodUnmanaged, 0, 0, ""
}

// validatePodContract enforces the canonical pod contract on a classified,
// managed pod: the creator must be the expected built-in controller, the pod
// must belong to the live cluster, and its full normalized form must equal the
// reconciler-stamped parent template plus a recomputed contract hash.
func (v *PodCreateValidator) validatePodContract(ctx context.Context, request admission.Request, pod *corev1.Pod, kind contractPodKind, shard, member int32, clusterName string) *admission.Response {
	creator := request.UserInfo.Username
	switch kind {
	case contractPodMember:
		if creator != v.identities.StatefulSetController {
			return deniedf("managed PostgreSQL member Pod must be created by the StatefulSet controller")
		}
	case contractPodPooler, contractPodOrchestrator:
		if creator != v.identities.ReplicaSetController {
			return deniedf("managed supporting Pod must be created by the ReplicaSet controller")
		}
	default:
		return deniedf("unclassified managed Pod")
	}

	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := v.reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: clusterName}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return deniedf("managed Pod's owning PgShardCluster no longer exists")
		}
		return erroredf("read PgShardCluster for Pod contract admission: %w", err)
	}
	if cluster.UID == "" {
		return deniedf("owning PgShardCluster has no stable UID")
	}
	if types.UID(pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation]) != cluster.UID {
		return deniedf("managed Pod does not belong to the live PgShardCluster UID")
	}

	class, template, provenance, response := v.resolveStampedParent(ctx, pod, kind, shard, member, clusterName, cluster)
	if response != nil {
		return response
	}

	nc := owned.NormContext{
		Class:       class,
		ClusterName: clusterName,
		Namespace:   pod.Namespace,
		Shard:       shard,
		Member:      member,
		Provenance:  provenance,
	}
	// enforceDigestPin is false until the activation stage pins protected image
	// digests; the comparator still requires an exact normalized-contract match.
	if err := owned.ComparePodToStampedTemplate(nc, pod.ObjectMeta, pod.Spec, template.ObjectMeta, template.Spec, owned.StageCreate, false); err != nil {
		return deniedf("managed Pod does not match its stamped contract: %v", err)
	}

	want := template.Annotations[owned.PodContractHashAnnotation]
	if want == "" {
		return deniedf("stamped parent template carries no contract hash")
	}
	if pod.Annotations[owned.PodContractHashAnnotation] != want {
		return deniedf("managed Pod contract hash does not match its stamped parent template")
	}
	generation, err := strconv.ParseInt(template.Annotations[owned.PodSecurityGenerationAnnotation], 10, 64)
	if err != nil {
		return deniedf("stamped parent template carries an invalid security generation")
	}
	got, err := owned.HashAdmittedPod(nc, pod.ObjectMeta, pod.Spec, owned.StageCreate, string(cluster.UID), generation)
	if err != nil {
		return erroredf("recompute managed Pod contract hash: %w", err)
	}
	if got != want {
		return deniedf("managed Pod contract hash recomputation does not match its stamped parent template")
	}
	return nil
}

// resolveStampedParent fetches the live controller parent whose stamped pod
// template a pod must match, returning the resolved class, that template, and
// the authoritative controller provenance the normalizer validates residue
// against.
func (v *PodCreateValidator) resolveStampedParent(ctx context.Context, pod *corev1.Pod, kind contractPodKind, shard, member int32, clusterName string, cluster *pgshardv1alpha1.PgShardCluster) (owned.PodClass, *corev1.PodTemplateSpec, *owned.ControllerEvidence, *admission.Response) {
	switch kind {
	case contractPodMember:
		statefulSetName := owned.PostgreSQLMemberStatefulSetName(clusterName, shard, member)
		statefulSet := &appsv1.StatefulSet{}
		if err := v.reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: statefulSetName}, statefulSet); err != nil {
			if apierrors.IsNotFound(err) {
				return "", nil, nil, deniedf("managed member Pod has no live owning StatefulSet")
			}
			return "", nil, nil, erroredf("read owning StatefulSet for Pod contract admission: %w", err)
		}
		if statefulSet.UID == "" || !isControlledBy(statefulSet.OwnerReferences, pgShardClusterKind, cluster.UID) {
			return "", nil, nil, deniedf("member StatefulSet is not owned by the live PgShardCluster")
		}
		provenance := &owned.ControllerEvidence{ParentUID: string(statefulSet.UID)}
		return owned.ClassForMember(cluster.Spec.MembersPerShard, member), &statefulSet.Spec.Template, provenance, nil
	case contractPodPooler, contractPodOrchestrator:
		class := owned.ClassPooler
		component := componentPooler
		if kind == contractPodOrchestrator {
			class = owned.ClassOrchestrator
			component = componentOrchestrator
		}
		replicaSetRef := controllerOwnerRef(pod.OwnerReferences)
		if replicaSetRef == nil || replicaSetRef.Kind != replicaSetKind {
			return "", nil, nil, deniedf("managed supporting Pod is not owned by a ReplicaSet")
		}
		replicaSet := &appsv1.ReplicaSet{}
		if err := v.reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: replicaSetRef.Name}, replicaSet); err != nil {
			if apierrors.IsNotFound(err) {
				return "", nil, nil, deniedf("managed supporting Pod's owning ReplicaSet no longer exists")
			}
			return "", nil, nil, erroredf("read owning ReplicaSet for Pod contract admission: %w", err)
		}
		if replicaSet.UID != replicaSetRef.UID {
			return "", nil, nil, deniedf("managed supporting Pod owner ReplicaSet UID mismatch")
		}
		deploymentRef := controllerOwnerRef(replicaSet.OwnerReferences)
		if deploymentRef == nil || deploymentRef.Kind != deploymentKind {
			return "", nil, nil, deniedf("supporting ReplicaSet is not owned by a Deployment")
		}
		deployment := &appsv1.Deployment{}
		if err := v.reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: deploymentRef.Name}, deployment); err != nil {
			if apierrors.IsNotFound(err) {
				return "", nil, nil, deniedf("supporting ReplicaSet's owning Deployment no longer exists")
			}
			return "", nil, nil, erroredf("read owning Deployment for Pod contract admission: %w", err)
		}
		if deployment.UID != deploymentRef.UID {
			return "", nil, nil, deniedf("supporting ReplicaSet owner Deployment UID mismatch")
		}
		if response := requireOperatorWorkload(deployment.ObjectMeta, component, cluster.UID); response != nil {
			return "", nil, nil, response
		}
		provenance := &owned.ControllerEvidence{
			ParentUID:       string(replicaSet.UID),
			ReplicaSetName:  replicaSet.Name,
			PodTemplateHash: replicaSet.Labels[podTemplateHashLabel],
		}
		return class, &replicaSet.Spec.Template, provenance, nil
	}
	return "", nil, nil, deniedf("unclassified managed Pod")
}

// WorkloadIntegrityValidator authenticates the authorship and contract stamp of
// managed apps workloads (StatefulSets, Deployments, ReplicaSets) and their
// scale subresources before they can produce pods.
type WorkloadIntegrityValidator struct {
	reader     client.Reader
	identities ControllerIdentities
	decoder    admission.Decoder
}

func NewWorkloadIntegrityValidator(reader client.Reader, identities ControllerIdentities, scheme *runtime.Scheme) *WorkloadIntegrityValidator {
	return &WorkloadIntegrityValidator{reader: reader, identities: identities, decoder: admission.NewDecoder(scheme)}
}

func (v *WorkloadIntegrityValidator) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Create && request.Operation != admissionv1.Update {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected workload integrity request %s", request.Operation))
	}
	if request.SubResource == "scale" {
		return v.handleScale(ctx, request)
	}
	if request.SubResource != "" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected workload subresource %q", request.SubResource))
	}
	switch request.Resource.Resource {
	case "statefulsets":
		return v.handleStatefulSet(ctx, request)
	case "deployments":
		return v.handleDeployment(ctx, request)
	case "replicasets":
		return v.handleReplicaSet(ctx, request)
	}
	return admission.Allowed("resource is not a managed pgshard workload")
}

func (v *WorkloadIntegrityValidator) handleStatefulSet(ctx context.Context, request admission.Request) admission.Response {
	statefulSet := &appsv1.StatefulSet{}
	if err := v.decoder.Decode(request, statefulSet); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode StatefulSet: %w", err))
	}
	if statefulSet.Labels[owned.ComponentLabel] != componentPostgreSQL || statefulSet.Labels[owned.ClusterLabel] == "" {
		return admission.Allowed("StatefulSet is not a managed pgshard member workload")
	}
	if err := decodeStrictObject(request.Object.Raw, &appsv1.StatefulSet{}); err != nil {
		return admission.Denied(fmt.Sprintf("managed member StatefulSet carries unknown or duplicate fields: %v", err))
	}
	if request.UserInfo.Username != v.identities.Operator {
		return admission.Denied("managed member StatefulSet may only be authored by the pgshard operator")
	}
	shard, shardOK := owned.ParseIdentityLabel(statefulSet.Labels[owned.ShardLabel])
	member, memberOK := owned.ParseIdentityLabel(statefulSet.Labels[owned.MemberLabel])
	if !shardOK || !memberOK {
		return admission.Denied("managed member StatefulSet carries malformed shard or member identity")
	}
	if statefulSet.Spec.Replicas != nil && *statefulSet.Spec.Replicas != 1 {
		return admission.Denied("managed member StatefulSet must declare exactly one replica")
	}
	cluster, response := v.boundCluster(ctx, statefulSet.Namespace, statefulSet.Labels[owned.ClusterLabel], statefulSet.OwnerReferences)
	if response != nil {
		return *response
	}
	class := owned.ClassForMember(cluster.Spec.MembersPerShard, member)
	if err := verifyTemplateStamp(class, string(cluster.UID), shard, member, &statefulSet.Spec.Template); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("managed member StatefulSet carries a valid contract stamp")
}

func (v *WorkloadIntegrityValidator) handleDeployment(ctx context.Context, request admission.Request) admission.Response {
	deployment := &appsv1.Deployment{}
	if err := v.decoder.Decode(request, deployment); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode Deployment: %w", err))
	}
	component := deployment.Labels[owned.ComponentLabel]
	if deployment.Labels[owned.ClusterLabel] == "" || (component != componentPooler && component != componentOrchestrator) {
		return admission.Allowed("Deployment is not a managed pgshard supporting workload")
	}
	if err := decodeStrictObject(request.Object.Raw, &appsv1.Deployment{}); err != nil {
		return admission.Denied(fmt.Sprintf("managed supporting Deployment carries unknown or duplicate fields: %v", err))
	}
	if request.UserInfo.Username != v.identities.Operator {
		return admission.Denied("managed supporting Deployment may only be authored by the pgshard operator")
	}
	cluster, response := v.boundCluster(ctx, deployment.Namespace, deployment.Labels[owned.ClusterLabel], deployment.OwnerReferences)
	if response != nil {
		return *response
	}
	class := owned.ClassPooler
	if component == componentOrchestrator {
		class = owned.ClassOrchestrator
	}
	if err := verifyTemplateStamp(class, string(cluster.UID), 0, 0, &deployment.Spec.Template); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("managed supporting Deployment carries a valid contract stamp")
}

func (v *WorkloadIntegrityValidator) handleReplicaSet(ctx context.Context, request admission.Request) admission.Response {
	replicaSet := &appsv1.ReplicaSet{}
	if err := v.decoder.Decode(request, replicaSet); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode ReplicaSet: %w", err))
	}
	component := replicaSet.Labels[owned.ComponentLabel]
	if replicaSet.Labels[owned.ClusterLabel] == "" || (component != componentPooler && component != componentOrchestrator) {
		return admission.Allowed("ReplicaSet is not a managed pgshard supporting workload")
	}
	if err := decodeStrictObject(request.Object.Raw, &appsv1.ReplicaSet{}); err != nil {
		return admission.Denied(fmt.Sprintf("managed supporting ReplicaSet carries unknown or duplicate fields: %v", err))
	}
	if request.UserInfo.Username != v.identities.DeploymentController {
		return admission.Denied("managed supporting ReplicaSet may only be authored by the Deployment controller")
	}
	deploymentRef := controllerOwnerRef(replicaSet.OwnerReferences)
	if deploymentRef == nil || deploymentRef.Kind != deploymentKind {
		return admission.Denied("managed supporting ReplicaSet is not owned by a Deployment")
	}
	deployment := &appsv1.Deployment{}
	if err := v.reader.Get(ctx, types.NamespacedName{Namespace: replicaSet.Namespace, Name: deploymentRef.Name}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return admission.Denied("managed supporting ReplicaSet's owning Deployment no longer exists")
		}
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("read owning Deployment for ReplicaSet admission: %w", err))
	}
	if deployment.UID != deploymentRef.UID {
		return admission.Denied("managed supporting ReplicaSet owner Deployment UID mismatch")
	}
	cluster, response := v.boundCluster(ctx, replicaSet.Namespace, replicaSet.Labels[owned.ClusterLabel], deployment.OwnerReferences)
	if response != nil {
		return *response
	}
	if response := requireOperatorWorkload(deployment.ObjectMeta, component, cluster.UID); response != nil {
		return *response
	}
	return admission.Allowed("managed supporting ReplicaSet inherits a valid Deployment contract")
}

// handleScale enforces the only scale bound this stage owns: a managed member
// StatefulSet must stay a single replica. Supporting Deployment/ReplicaSet
// autoscaling bounds are enforced by a later activation stage together with the
// HorizontalPodAutoscaler identity probe.
func (v *WorkloadIntegrityValidator) handleScale(ctx context.Context, request admission.Request) admission.Response {
	if request.Resource.Resource != "statefulsets" {
		return admission.Allowed("supporting workload scale bounds are enforced by a later activation stage")
	}
	scale := &autoscalingv1.Scale{}
	if err := decodeStrictObject(request.Object.Raw, scale); err != nil {
		return admission.Denied(fmt.Sprintf("scale request carries unknown or duplicate fields: %v", err))
	}
	statefulSet := &appsv1.StatefulSet{}
	if err := v.reader.Get(ctx, types.NamespacedName{Namespace: request.Namespace, Name: request.Name}, statefulSet); err != nil {
		if apierrors.IsNotFound(err) {
			return admission.Allowed("scale target no longer exists")
		}
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("read scale target StatefulSet: %w", err))
	}
	if statefulSet.Labels[owned.ComponentLabel] != componentPostgreSQL || statefulSet.Labels[owned.ClusterLabel] == "" {
		return admission.Allowed("scale target is not a managed pgshard member workload")
	}
	if scale.Spec.Replicas != 1 {
		return admission.Denied("managed member StatefulSet must remain a single replica")
	}
	return admission.Allowed("managed member StatefulSet scale is within bounds")
}

func (v *WorkloadIntegrityValidator) boundCluster(ctx context.Context, namespace, clusterName string, ownerRefs []metav1.OwnerReference) (*pgshardv1alpha1.PgShardCluster, *admission.Response) {
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := v.reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: clusterName}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, deniedf("workload's owning PgShardCluster no longer exists")
		}
		return nil, erroredf("read PgShardCluster for workload admission: %w", err)
	}
	if cluster.UID == "" {
		return nil, deniedf("owning PgShardCluster has no stable UID")
	}
	if !isControlledBy(ownerRefs, pgShardClusterKind, cluster.UID) {
		return nil, deniedf("workload is not owned by the live PgShardCluster")
	}
	return cluster, nil
}

func verifyTemplateStamp(class owned.PodClass, clusterUID string, shard, member int32, template *corev1.PodTemplateSpec) error {
	want := template.Annotations[owned.PodContractHashAnnotation]
	if want == "" {
		return fmt.Errorf("workload pod template carries no contract stamp")
	}
	generation, err := strconv.ParseInt(template.Annotations[owned.PodSecurityGenerationAnnotation], 10, 64)
	if err != nil {
		return fmt.Errorf("workload pod template carries an invalid security generation")
	}
	got, err := owned.ComputeContractStamp(class, clusterUID, shard, member, generation, template)
	if err != nil {
		return fmt.Errorf("recompute workload contract stamp: %w", err)
	}
	if got != want {
		return fmt.Errorf("workload pod template contract stamp does not recompute")
	}
	return nil
}

func requireOperatorWorkload(meta metav1.ObjectMeta, component string, clusterUID types.UID) *admission.Response {
	if meta.Labels[owned.ManagedByLabel] != owned.ManagedByValue {
		return deniedf("workload is not operator-managed")
	}
	if meta.Labels[owned.ComponentLabel] != component {
		return deniedf("workload component does not match its ReplicaSet")
	}
	if !isControlledBy(meta.OwnerReferences, pgShardClusterKind, clusterUID) {
		return deniedf("workload is not owned by the live PgShardCluster")
	}
	return nil
}

func controllerOwnerRef(refs []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}

func isControlledBy(refs []metav1.OwnerReference, kind string, uid types.UID) bool {
	ref := controllerOwnerRef(refs)
	return ref != nil && ref.Kind == kind && ref.UID == uid
}

// decodeStrictObject rejects raw admission JSON that carries fields unknown to
// the typed object or duplicate keys, closing the gap between a lenient typed
// decode and the exact object the API server will persist.
func decodeStrictObject(raw []byte, into any) error {
	strictErrs, err := sigsjson.UnmarshalStrict(raw, into)
	if err != nil {
		return err
	}
	if len(strictErrs) > 0 {
		return errors.Join(strictErrs...)
	}
	return nil
}

func deniedf(format string, args ...any) *admission.Response {
	response := admission.Denied(fmt.Sprintf(format, args...))
	return &response
}

func erroredf(format string, args ...any) *admission.Response {
	response := admission.Errored(http.StatusInternalServerError, fmt.Errorf(format, args...))
	return &response
}
