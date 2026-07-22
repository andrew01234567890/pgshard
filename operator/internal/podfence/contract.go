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
	"k8s.io/apimachinery/pkg/api/equality"
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

	podTemplateHashLabel        = "pod-template-hash"
	controllerRevisionHashLabel = "controller-revision-hash"
	componentPostgreSQL         = "postgresql"
	componentPooler             = "pooler"
	componentOrchestrator       = "orchestrator"

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

// isManagedLooking reports whether a pod carries the surface shape of a managed
// pod (a cluster label plus a managed component), regardless of whether its
// shard/member identity is canonical. A managed-looking pod that classifies as
// unmanaged is malformed and must be rejected, never silently treated as an
// ordinary foreign pod.
func isManagedLooking(pod *corev1.Pod) bool {
	if pod.Labels[owned.ClusterLabel] == "" {
		return false
	}
	switch pod.Labels[owned.ComponentLabel] {
	case componentPostgreSQL, componentPooler, componentOrchestrator:
		return true
	}
	return false
}

// validatePodContract enforces the canonical pod contract on a classified,
// managed pod: the creator must be the expected built-in controller, the pod
// must belong to the live cluster, and its full normalized form must equal the
// reconciler-stamped parent template plus a recomputed contract hash.
func (v *PodCreateValidator) validatePodContract(ctx context.Context, request admission.Request, pod *corev1.Pod, kind contractPodKind, shard, member int32, clusterName string, enforceDigestPin bool) *admission.Response {
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

	namespace := request.Namespace
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := v.reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: clusterName}, cluster); err != nil {
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

	class, template, provenance, response := resolveStampedParent(ctx, v.reader, namespace, pod, kind, shard, member, clusterName, cluster)
	if response != nil {
		return response
	}

	templateGeneration := template.Annotations[owned.PodSecurityGenerationAnnotation]
	if pod.Annotations[owned.PodSecurityGenerationAnnotation] != templateGeneration {
		return deniedf("managed Pod security generation does not match its stamped parent template")
	}
	generation, ok := canonicalSecurityGeneration(templateGeneration)
	if !ok {
		return deniedf("stamped parent template carries an invalid security generation")
	}

	nc := owned.NormContext{
		Class:       class,
		ClusterName: clusterName,
		Namespace:   namespace,
		Shard:       shard,
		Member:      member,
		Provenance:  provenance,
	}
	// enforceDigestPin is true only once isolation is ACTIVE; the comparator
	// always requires an exact normalized-contract match either way.
	if err := owned.ComparePodToStampedTemplate(nc, pod.ObjectMeta, pod.Spec, template.ObjectMeta, template.Spec, owned.StageCreate, enforceDigestPin); err != nil {
		return deniedf("managed Pod does not match its stamped contract: %v", err)
	}

	want := template.Annotations[owned.PodContractHashAnnotation]
	if want == "" {
		return deniedf("stamped parent template carries no contract hash")
	}
	if pod.Annotations[owned.PodContractHashAnnotation] != want {
		return deniedf("managed Pod contract hash does not match its stamped parent template")
	}
	got, err := owned.HashAdmittedPod(nc, pod.ObjectMeta, pod.Spec, owned.StageCreate, string(cluster.UID), generation)
	if err != nil {
		return erroredf("recompute managed Pod contract hash: %w", err)
	}
	if got != want {
		return deniedf("managed Pod contract hash recomputation does not match its stamped parent template")
	}
	if response := validateSupportingAdmission(cluster, kind, class, provenance, &pod.Spec, want, generation); response != nil {
		return response
	}
	return nil
}

// validateSupportingAdmission layers the always-on security floor and the
// per-class generation compare-and-set barrier onto the comparator/provenance
// validation of a supporting pod. Members are unaffected.
func validateSupportingAdmission(cluster *pgshardv1alpha1.PgShardCluster, kind contractPodKind, class owned.PodClass, provenance *owned.ControllerEvidence, spec *corev1.PodSpec, stampedHash string, stampedGeneration int64) *admission.Response {
	if kind != contractPodPooler && kind != contractPodOrchestrator {
		return nil
	}
	if err := validateSupportingSecurityFloor(spec); err != nil {
		return deniedf("managed supporting Pod violates the security floor: %v", err)
	}
	return validateSupportingGenerationBarrier(cluster, string(class), provenance.ParentUID, stampedHash, stampedGeneration)
}

// validateSupportingSecurityFloor enforces the class-independent, always-on
// invariants that must hold on the live supporting pod regardless of its stamp:
// zero host-namespace surface, no hostPath volume, and no debug ephemeral
// container. The exact service account, automount setting, image digest pin, and
// guarded volume/Secret set are already held to the stamped template by the
// full-spec comparator; this floor is defense in depth for the host surface that
// most directly bridges the isolation boundary.
func validateSupportingSecurityFloor(spec *corev1.PodSpec) error {
	if spec.HostNetwork || spec.HostPID || spec.HostIPC {
		return fmt.Errorf("supporting Pod must not share the host network, PID, or IPC namespace")
	}
	if spec.HostUsers != nil && *spec.HostUsers {
		return fmt.Errorf("supporting Pod must not share the host user namespace")
	}
	for i := range spec.Volumes {
		if spec.Volumes[i].HostPath != nil {
			return fmt.Errorf("supporting Pod must not mount a hostPath volume")
		}
	}
	if len(spec.EphemeralContainers) != 0 {
		return fmt.Errorf("supporting Pod must not carry ephemeral containers")
	}
	return nil
}

// validateSupportingGenerationBarrier decides which sealed ReplicaSet generation
// a supporting pod may belong to. It reads the class's compare-and-set record: a
// pod stamped below MinGenerationForNewCreates is a revoked/downgrade create, and
// a pod must belong to the current or the (still-uncleared) prior ReplicaSet with
// that generation's exact contract hash. When no record is sealed yet, the
// barrier defers to the inventory-gated activation stage.
func validateSupportingGenerationBarrier(cluster *pgshardv1alpha1.PgShardCluster, class, replicaSetUID, stampedHash string, stampedGeneration int64) *admission.Response {
	record := supportingGenerationRecord(cluster, class)
	if record == nil || record.CurrentReplicaSetUID == "" {
		return nil
	}
	if stampedGeneration < record.MinGenerationForNewCreates {
		return deniedf("managed supporting Pod security generation %d is below the revocation barrier %d", stampedGeneration, record.MinGenerationForNewCreates)
	}
	switch replicaSetUID {
	case record.CurrentReplicaSetUID:
		if stampedHash != record.CurrentContractHash {
			return deniedf("managed supporting Pod does not match the current sealed generation")
		}
	case record.PriorReplicaSetUID:
		if stampedHash != record.PriorContractHash {
			return deniedf("managed supporting Pod does not match the prior sealed generation")
		}
	default:
		return deniedf("managed supporting Pod belongs to a ReplicaSet outside the sealed generation set")
	}
	return nil
}

func supportingGenerationRecord(cluster *pgshardv1alpha1.PgShardCluster, class string) *pgshardv1alpha1.SupportingGenerationStatus {
	for i := range cluster.Status.SupportingGenerations {
		if cluster.Status.SupportingGenerations[i].Class == class {
			return &cluster.Status.SupportingGenerations[i]
		}
	}
	return nil
}

// resolveStampedParent fetches the live controller parent whose stamped pod
// template a pod must match, returning the resolved class, that template, and
// the authoritative controller provenance the normalizer validates residue
// against. All evidence is live: the member pod's controller-revision-hash must
// match the StatefulSet's recorded revision state, and the supporting evidence
// comes from the owning ReplicaSet's own pod-template-hash label, which must be
// present.
func resolveStampedParent(ctx context.Context, reader client.Reader, namespace string, pod *corev1.Pod, kind contractPodKind, shard, member int32, clusterName string, cluster *pgshardv1alpha1.PgShardCluster) (owned.PodClass, *corev1.PodTemplateSpec, *owned.ControllerEvidence, *admission.Response) {
	switch kind {
	case contractPodMember:
		statefulSetName := owned.PostgreSQLMemberStatefulSetName(clusterName, shard, member)
		statefulSet := &appsv1.StatefulSet{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: statefulSetName}, statefulSet); err != nil {
			if apierrors.IsNotFound(err) {
				return "", nil, nil, deniedf("managed member Pod has no live owning StatefulSet")
			}
			return "", nil, nil, erroredf("read owning StatefulSet for Pod contract admission: %w", err)
		}
		if statefulSet.UID == "" || !isControlledBy(statefulSet.OwnerReferences, pgShardClusterKind, cluster.UID) {
			return "", nil, nil, deniedf("member StatefulSet is not owned by the live PgShardCluster")
		}
		revision := pod.Labels[controllerRevisionHashLabel]
		if revision == "" {
			return "", nil, nil, deniedf("managed member Pod carries no controller revision evidence")
		}
		current, updated := statefulSet.Status.CurrentRevision, statefulSet.Status.UpdateRevision
		if (updated == "" || revision != updated) && (current == "" || revision != current) {
			return "", nil, nil, deniedf("managed member Pod revision does not match the live StatefulSet revision state")
		}
		provenance := &owned.ControllerEvidence{ParentUID: string(statefulSet.UID), ControllerRevisionHash: revision}
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
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: replicaSetRef.Name}, replicaSet); err != nil {
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
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: deploymentRef.Name}, deployment); err != nil {
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
		templateHash := replicaSet.Labels[podTemplateHashLabel]
		if templateHash == "" {
			return "", nil, nil, deniedf("owning ReplicaSet carries no pod-template-hash evidence")
		}
		provenance := &owned.ControllerEvidence{
			ParentUID:       string(replicaSet.UID),
			ReplicaSetName:  replicaSet.Name,
			PodTemplateHash: templateHash,
		}
		return class, &replicaSet.Spec.Template, provenance, nil
	}
	return "", nil, nil, deniedf("unclassified managed Pod")
}

// validateBoundPodContract runs the LIVE-mode contract comparison for a stamped
// pod at the moment its Node identity is bound. It projects the pod as it will
// exist once bound to the selected Node — nodeName, node-UID/boot-ID
// annotations, and the Node's topology labels are all derived authoritatively
// from the live Node, never trusted from the pod — then requires that
// projection to normalize exactly equal to the pod's stamped parent template.
// A pod that carries no reconciler stamp is left to the pre-activation legacy
// path (nil). The pre-bind pod must carry no node residue of its own.
func validateBoundPodContract(ctx context.Context, reader client.Reader, pod *corev1.Pod, node *corev1.Node, cluster *pgshardv1alpha1.PgShardCluster, enforceDigestPin bool) *admission.Response {
	if pod.Annotations[owned.PodContractHashAnnotation] == "" {
		return nil
	}
	kind, shard, member, clusterName := classifyContractPod(pod)
	if kind == contractPodUnmanaged {
		return nil
	}
	if pod.Annotations[NodeUIDAnnotation] != "" || pod.Annotations[NodeBootIDAnnotation] != "" {
		return deniedf("managed Pod carries node identity residue before it is bound")
	}
	for _, key := range []string{corev1.LabelTopologyZone, corev1.LabelTopologyRegion} {
		if _, present := pod.Labels[key]; present {
			return deniedf("managed Pod carries a topology label before it is bound")
		}
	}

	class, template, provenance, response := resolveStampedParent(ctx, reader, pod.Namespace, pod, kind, shard, member, clusterName, cluster)
	if response != nil {
		return response
	}
	templateGeneration := template.Annotations[owned.PodSecurityGenerationAnnotation]
	if pod.Annotations[owned.PodSecurityGenerationAnnotation] != templateGeneration {
		return deniedf("managed Pod security generation does not match its stamped parent template")
	}
	generation, ok := canonicalSecurityGeneration(templateGeneration)
	if !ok {
		return deniedf("stamped parent template carries an invalid security generation")
	}

	evidence := &owned.BindingEvidence{
		NodeName: node.Name,
		NodeUID:  string(node.UID),
		BootID:   node.Status.NodeInfo.BootID,
		Zone:     node.Labels[corev1.LabelTopologyZone],
		Region:   node.Labels[corev1.LabelTopologyRegion],
	}
	bound := projectBoundPod(pod, evidence)
	nc := owned.NormContext{
		Class:       class,
		ClusterName: clusterName,
		Namespace:   pod.Namespace,
		Shard:       shard,
		Member:      member,
		Provenance:  provenance,
		Binding:     evidence,
	}
	if err := owned.ComparePodToStampedTemplate(nc, bound.ObjectMeta, bound.Spec, template.ObjectMeta, template.Spec, owned.StageLive, enforceDigestPin); err != nil {
		return deniedf("bound managed Pod does not match its stamped contract: %v", err)
	}
	want := template.Annotations[owned.PodContractHashAnnotation]
	if pod.Annotations[owned.PodContractHashAnnotation] != want {
		return deniedf("bound managed Pod contract hash does not match its stamped parent template")
	}
	got, err := owned.HashAdmittedPod(nc, bound.ObjectMeta, bound.Spec, owned.StageLive, string(cluster.UID), generation)
	if err != nil {
		return erroredf("recompute bound managed Pod contract hash: %w", err)
	}
	if got != want {
		return deniedf("bound managed Pod contract hash recomputation does not match its stamped parent template")
	}
	if response := validateSupportingAdmission(cluster, kind, class, provenance, &pod.Spec, want, generation); response != nil {
		return response
	}
	return nil
}

// projectBoundPod returns the pod as it will exist once the API server commits
// the binding: assigned to the Node and carrying only the Node's authoritative
// incarnation and topology residue.
func projectBoundPod(pod *corev1.Pod, evidence *owned.BindingEvidence) *corev1.Pod {
	bound := pod.DeepCopy()
	bound.Spec.NodeName = evidence.NodeName
	if bound.Annotations == nil {
		bound.Annotations = map[string]string{}
	}
	bound.Annotations[NodeUIDAnnotation] = evidence.NodeUID
	bound.Annotations[NodeBootIDAnnotation] = evidence.BootID
	if bound.Labels == nil {
		bound.Labels = map[string]string{}
	}
	for key, value := range map[string]string{corev1.LabelTopologyZone: evidence.Zone, corev1.LabelTopologyRegion: evidence.Region} {
		if value == "" {
			delete(bound.Labels, key)
		} else {
			bound.Labels[key] = value
		}
	}
	return bound
}

// WorkloadIntegrityValidator authenticates the authorship, identity, and
// contract stamp of apps workloads (StatefulSets, Deployments, ReplicaSets) and
// their scale subresources in fenced namespaces. Fenced namespaces are
// dedicated: no legitimate non-pgshard apps object exists there, so authorship
// is gated by namespace, never by the object's own labels — classification is
// evidence, not a trust boundary.
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
	if request.Operation == admissionv1.Create {
		receipt, err := namespaceIsolationReceipt(ctx, v.reader, request.Namespace)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, err)
		}
		// During QUIESCE the namespace is frozen: no new workload may be created
		// while the reconciler seals parents and drains. RECREATE deliberately
		// still allows the CAS-roll ReplicaSet create (subject to the usual
		// authorship checks below) so supporting pods can be re-authenticated.
		if isolationPhase(receipt) == pgshardv1alpha1.IsolationActivatingQuiesce {
			return admission.Denied("namespace isolation is quiescing; workload creation is frozen")
		}
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
	return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected workload resource %q", request.Resource.Resource))
}

// handleStatefulSet admits StatefulSet writes in a fenced namespace: only the
// operator authors them (the StatefulSet controller writes status and pods,
// never the main resource), identity transitions are denied, and a managed
// member workload must carry a recomputable contract stamp on a single replica.
func (v *WorkloadIntegrityValidator) handleStatefulSet(ctx context.Context, request admission.Request) admission.Response {
	statefulSet := &appsv1.StatefulSet{}
	if err := decodeStrictObject(request.Object.Raw, statefulSet); err != nil {
		return admission.Denied(fmt.Sprintf("StatefulSet in a fenced namespace carries unknown or duplicate fields: %v", err))
	}
	if request.UserInfo.Username != v.identities.Operator {
		return admission.Denied("StatefulSets in a fenced namespace may only be authored by the pgshard operator")
	}
	if request.Operation == admissionv1.Update {
		old := &appsv1.StatefulSet{}
		if err := v.decoder.DecodeRaw(request.OldObject, old); err != nil {
			return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode prior StatefulSet: %w", err))
		}
		if response := denyProtectedIdentityTransition(old.ObjectMeta, statefulSet.ObjectMeta, isMemberWorkloadLabels); response != nil {
			return *response
		}
	}
	if !isMemberWorkloadLabels(statefulSet.Labels) {
		return admission.Allowed("operator-authored StatefulSet carries no managed identity")
	}
	shard, shardOK := owned.ParseIdentityLabel(statefulSet.Labels[owned.ShardLabel])
	member, memberOK := owned.ParseIdentityLabel(statefulSet.Labels[owned.MemberLabel])
	if !shardOK || !memberOK {
		return admission.Denied("managed member StatefulSet carries malformed shard or member identity")
	}
	if statefulSet.Spec.Replicas != nil && *statefulSet.Spec.Replicas != 1 {
		return admission.Denied("managed member StatefulSet must declare exactly one replica")
	}
	cluster, response := v.boundCluster(ctx, request.Namespace, statefulSet.Labels[owned.ClusterLabel], statefulSet.OwnerReferences)
	if response != nil {
		return *response
	}
	class := owned.ClassForMember(cluster.Spec.MembersPerShard, member)
	if err := verifyTemplateStamp(class, string(cluster.UID), shard, member, &statefulSet.Spec.Template); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("managed member StatefulSet carries a valid contract stamp")
}

// handleDeployment admits Deployment writes in a fenced namespace. The operator
// is the sole author; the deployment controller may only perform invariant
// updates (its revision-annotation sync) that leave identity and the pod
// template untouched.
func (v *WorkloadIntegrityValidator) handleDeployment(ctx context.Context, request admission.Request) admission.Response {
	deployment := &appsv1.Deployment{}
	if err := decodeStrictObject(request.Object.Raw, deployment); err != nil {
		return admission.Denied(fmt.Sprintf("Deployment in a fenced namespace carries unknown or duplicate fields: %v", err))
	}
	if request.Operation == admissionv1.Update && request.UserInfo.Username == v.identities.DeploymentController {
		old := &appsv1.Deployment{}
		if err := v.decoder.DecodeRaw(request.OldObject, old); err != nil {
			return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode prior Deployment: %w", err))
		}
		if workloadIdentityChanged(old.ObjectMeta, deployment.ObjectMeta) || !equality.Semantic.DeepEqual(old.Spec.Template, deployment.Spec.Template) {
			return admission.Denied("the Deployment controller may not mutate a fenced Deployment's identity or pod template")
		}
		return admission.Allowed("Deployment controller update leaves identity and pod template unchanged")
	}
	if request.UserInfo.Username != v.identities.Operator {
		return admission.Denied("Deployments in a fenced namespace may only be authored by the pgshard operator")
	}
	if request.Operation == admissionv1.Update {
		old := &appsv1.Deployment{}
		if err := v.decoder.DecodeRaw(request.OldObject, old); err != nil {
			return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode prior Deployment: %w", err))
		}
		if response := denyProtectedIdentityTransition(old.ObjectMeta, deployment.ObjectMeta, isSupportingWorkloadLabels); response != nil {
			return *response
		}
	}
	if !isSupportingWorkloadLabels(deployment.Labels) {
		return admission.Allowed("operator-authored Deployment carries no managed identity")
	}
	cluster, response := v.boundCluster(ctx, request.Namespace, deployment.Labels[owned.ClusterLabel], deployment.OwnerReferences)
	if response != nil {
		return *response
	}
	class := supportingClassForComponent(deployment.Labels[owned.ComponentLabel])
	if err := verifyTemplateStamp(class, string(cluster.UID), 0, 0, &deployment.Spec.Template); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("managed supporting Deployment carries a valid contract stamp")
}

// handleReplicaSet admits ReplicaSet writes in a fenced namespace. Creation is
// reserved to the deployment controller, and the created template must reduce
// to the exact stamped template of its live, operator-authored owning
// Deployment — a rogue ReplicaSet cannot launder an attacker template through
// the trusted pod-creating controllers regardless of its own labels. Updates
// (scaling, rollout annotations) must leave identity and the template
// untouched, so an old-generation ReplicaSet keeps scaling down during a
// rollout without ever changing what it produces.
func (v *WorkloadIntegrityValidator) handleReplicaSet(ctx context.Context, request admission.Request) admission.Response {
	replicaSet := &appsv1.ReplicaSet{}
	if err := decodeStrictObject(request.Object.Raw, replicaSet); err != nil {
		return admission.Denied(fmt.Sprintf("ReplicaSet in a fenced namespace carries unknown or duplicate fields: %v", err))
	}
	if request.UserInfo.Username != v.identities.DeploymentController {
		return admission.Denied("ReplicaSets in a fenced namespace may only be authored by the Deployment controller")
	}
	if request.Operation == admissionv1.Update {
		old := &appsv1.ReplicaSet{}
		if err := v.decoder.DecodeRaw(request.OldObject, old); err != nil {
			return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode prior ReplicaSet: %w", err))
		}
		if workloadIdentityChanged(old.ObjectMeta, replicaSet.ObjectMeta) {
			return admission.Denied("a fenced ReplicaSet's identity is immutable")
		}
		if !equality.Semantic.DeepEqual(old.Spec.Template, replicaSet.Spec.Template) {
			return admission.Denied("a fenced ReplicaSet's pod template is immutable")
		}
		return admission.Allowed("ReplicaSet update leaves identity and pod template unchanged")
	}
	deploymentRef := controllerOwnerRef(replicaSet.OwnerReferences)
	if deploymentRef == nil || deploymentRef.Kind != deploymentKind {
		return admission.Denied("a fenced ReplicaSet must be controller-owned by a Deployment")
	}
	deployment := &appsv1.Deployment{}
	if err := v.reader.Get(ctx, types.NamespacedName{Namespace: request.Namespace, Name: deploymentRef.Name}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return admission.Denied("a fenced ReplicaSet's owning Deployment no longer exists")
		}
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("read owning Deployment for ReplicaSet admission: %w", err))
	}
	if deployment.UID != deploymentRef.UID {
		return admission.Denied("fenced ReplicaSet owner Deployment UID mismatch")
	}
	if !isSupportingWorkloadLabels(deployment.Labels) {
		return admission.Denied("a fenced ReplicaSet's owning Deployment carries no managed identity")
	}
	component := deployment.Labels[owned.ComponentLabel]
	cluster, response := v.boundCluster(ctx, request.Namespace, deployment.Labels[owned.ClusterLabel], deployment.OwnerReferences)
	if response != nil {
		return *response
	}
	if response := requireOperatorWorkload(deployment.ObjectMeta, component, cluster.UID); response != nil {
		return *response
	}
	class := supportingClassForComponent(component)
	if err := verifyTemplateStamp(class, string(cluster.UID), 0, 0, &deployment.Spec.Template); err != nil {
		return admission.Denied(err.Error())
	}
	for _, key := range []string{owned.PodContractHashAnnotation, owned.PodSecurityGenerationAnnotation} {
		if replicaSet.Spec.Template.Annotations[key] != deployment.Spec.Template.Annotations[key] {
			return admission.Denied("fenced ReplicaSet contract stamp does not match its owning Deployment")
		}
	}
	generation, _ := canonicalSecurityGeneration(deployment.Spec.Template.Annotations[owned.PodSecurityGenerationAnnotation])
	replicaSetStamp, err := owned.ComputeContractStamp(class, string(cluster.UID), 0, 0, generation, &replicaSet.Spec.Template)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("recompute ReplicaSet contract stamp: %w", err))
	}
	if replicaSetStamp != deployment.Spec.Template.Annotations[owned.PodContractHashAnnotation] {
		return admission.Denied("fenced ReplicaSet pod template diverges from its stamped owning Deployment")
	}
	return admission.Allowed("fenced ReplicaSet inherits its owning Deployment's stamped contract")
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
	if !isMemberWorkloadLabels(statefulSet.Labels) {
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

func isMemberWorkloadLabels(labels map[string]string) bool {
	return labels[owned.ClusterLabel] != "" && labels[owned.ComponentLabel] == componentPostgreSQL
}

func isSupportingWorkloadLabels(labels map[string]string) bool {
	component := labels[owned.ComponentLabel]
	return labels[owned.ClusterLabel] != "" && (component == componentPooler || component == componentOrchestrator)
}

func supportingClassForComponent(component string) owned.PodClass {
	if component == componentOrchestrator {
		return owned.ClassOrchestrator
	}
	return owned.ClassPooler
}

// workloadIdentityLabels are the labels that define a workload's protected
// identity; together with the controller owner reference they may never change
// on an UPDATE in a fenced namespace.
var workloadIdentityLabels = []string{owned.ManagedByLabel, owned.ClusterLabel, owned.ComponentLabel, owned.ShardLabel, owned.MemberLabel}

func workloadIdentityChanged(old, updated metav1.ObjectMeta) bool {
	for _, key := range workloadIdentityLabels {
		if old.Labels[key] != updated.Labels[key] {
			return true
		}
	}
	oldRef, newRef := controllerOwnerRef(old.OwnerReferences), controllerOwnerRef(updated.OwnerReferences)
	if (oldRef == nil) != (newRef == nil) {
		return true
	}
	if oldRef != nil && (oldRef.Kind != newRef.Kind || oldRef.Name != newRef.Name || oldRef.UID != newRef.UID) {
		return true
	}
	return false
}

// denyProtectedIdentityTransition denies an UPDATE that moves an object into or
// out of protected identity, or changes the identity of a protected object.
// The stamp annotations are deliberately not part of the transition set: the
// authoring operator re-stamps templates on security-generation bumps and adds
// the stamp to pre-stamp workloads on upgrade.
func denyProtectedIdentityTransition(old, updated metav1.ObjectMeta, protected func(map[string]string) bool) *admission.Response {
	oldProtected, newProtected := protected(old.Labels), protected(updated.Labels)
	if oldProtected != newProtected {
		return deniedf("workloads in a fenced namespace may not transition into or out of managed identity")
	}
	if newProtected && workloadIdentityChanged(old, updated) {
		return deniedf("managed workload identity is immutable")
	}
	return nil
}

func verifyTemplateStamp(class owned.PodClass, clusterUID string, shard, member int32, template *corev1.PodTemplateSpec) error {
	want := template.Annotations[owned.PodContractHashAnnotation]
	if want == "" {
		return fmt.Errorf("workload pod template carries no contract stamp")
	}
	generation, ok := canonicalSecurityGeneration(template.Annotations[owned.PodSecurityGenerationAnnotation])
	if !ok {
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

// canonicalSecurityGeneration parses a security-generation annotation, accepting
// only the canonical decimal form of a positive int64 (no signs, padding, or
// whitespace).
func canonicalSecurityGeneration(raw string) (int64, bool) {
	generation, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || generation < 1 || strconv.FormatInt(generation, 10) != raw {
		return 0, false
	}
	return generation, true
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
// decode and the exact object the API server will persist. It decodes into the
// target as a side effect.
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
