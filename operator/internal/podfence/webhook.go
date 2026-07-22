// Package podfence authenticates the Kubernetes lifecycle evidence used before
// a managed PostgreSQL Pod finalizer may release its data.
package podfence

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	NamespaceLabel      = "pgshard.io/pod-fencing"
	NamespaceLabelValue = "enabled"

	NodeUIDAnnotation            = owned.PostgreSQLNodeUIDAnnotation
	NodeBootIDAnnotation         = owned.PostgreSQLNodeBootIDAnnotation
	HandshakeChallengeAnnotation = pgshardv1alpha1.PodFencingChallengeAnnotation
	HandshakeReceiptAnnotation   = pgshardv1alpha1.PodFencingReceiptAnnotation

	TerminationConditionType corev1.PodConditionType = "pgshard.io/PostgreSQLProcessTerminated"
	TerminationReason                                = "AuthenticatedKubelet"

	BindingWebhookName           = "mpostgresqlpodbinding.pgshard.io"
	BindingWebhookPath           = "/mutate-core-v1-postgresqlpodbinding"
	BindingValidationWebhookName = "vpostgresqlpodbinding.pgshard.io"
	BindingValidationWebhookPath = "/validate-core-v1-postgresqlpodbinding"
	StatusWebhookName            = "mpostgresqlpodstatus.pgshard.io"
	StatusWebhookPath            = "/mutate-core-v1-postgresqlpodstatus"
	StatusValidationWebhookName  = "vpostgresqlpodstatus.pgshard.io"
	StatusValidationWebhookPath  = "/validate-core-v1-postgresqlpodstatus"
	HandshakeWebhookName         = "mpostgresqlfencinghandshake.pgshard.io"
	HandshakeWebhookPath         = "/mutate-pgshard-io-v1alpha1-postgresqlfencinghandshake"
	MetadataWebhookName          = "vpostgresqlpodmetadata.pgshard.io"
	MetadataWebhookPath          = "/validate-core-v1-postgresqlpodmetadata"
	NamespaceWebhookName         = "vpostgresqlfencingnamespace.pgshard.io"
	NamespaceWebhookPath         = "/validate-core-v1-postgresqlfencingnamespace"
	PodCreateWebhookName         = "vpostgresqlpodcreate.pgshard.io"
	PodCreateWebhookPath         = "/validate-core-v1-postgresqlpodcreate"

	PodConnectWebhookPath        = "/validate-core-v1-postgresqlpodconnect"
	PodConnectFencedWebhookName  = "vpostgresqlpodconnect.pgshard.io"
	PodConnectManagerWebhookName = "vpostgresqlmanagerconnect.pgshard.io"
)

type HandshakeAttestor struct {
	decoder admission.Decoder
	codec   *HandshakeCodec
}

func NewHandshakeAttestor(codec *HandshakeCodec, scheme *runtime.Scheme) *HandshakeAttestor {
	return &HandshakeAttestor{decoder: admission.NewDecoder(scheme), codec: codec}
}

func (a *HandshakeAttestor) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Update || request.SubResource != "" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL fencing handshake request %s %q", request.Operation, request.SubResource))
	}
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := a.decoder.Decode(request, cluster); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode PgShardCluster fencing handshake: %w", err))
	}
	challenge := cluster.Annotations[HandshakeChallengeAnnotation]
	if challenge == "" {
		return admission.Allowed("PostgreSQL Pod fencing handshake is unchanged")
	}
	receipt, err := a.codec.Receipt(ctx, cluster)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("authenticate PgShardCluster fencing handshake: %w", err))
	}
	if cluster.Annotations[HandshakeReceiptAnnotation] == receipt {
		return admission.Allowed("PostgreSQL Pod fencing handshake is unchanged")
	}
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string, 1)
	}
	cluster.Annotations[HandshakeReceiptAnnotation] = receipt
	marshaled, err := json.Marshal(cluster)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("encode PgShardCluster fencing handshake: %w", err))
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaled)
}

type BindingAttestor struct {
	reader  client.Reader
	decoder admission.Decoder
}

type BindingValidator struct {
	reader  client.Reader
	decoder admission.Decoder
}

type bindingEvidence struct {
	pod     *corev1.Pod
	node    *corev1.Node
	cluster *pgshardv1alpha1.PgShardCluster
}

func NewBindingAttestor(reader client.Reader, scheme *runtime.Scheme) *BindingAttestor {
	return &BindingAttestor{reader: reader, decoder: admission.NewDecoder(scheme)}
}

func NewBindingValidator(reader client.Reader, scheme *runtime.Scheme) *BindingValidator {
	return &BindingValidator{reader: reader, decoder: admission.NewDecoder(scheme)}
}

func (a *BindingAttestor) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Create || request.SubResource != "binding" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL Pod binding request %s %q", request.Operation, request.SubResource))
	}
	binding := &corev1.Binding{}
	if err := a.decoder.Decode(request, binding); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode Pod binding: %w", err))
	}
	evidence, response := readBindingEvidence(ctx, a.reader, request, binding)
	if response != nil {
		return *response
	}
	if binding.Annotations == nil {
		binding.Annotations = make(map[string]string, 3)
	}
	if binding.Labels == nil {
		binding.Labels = make(map[string]string, 6)
	}
	for _, key := range protectedBindingLabels() {
		if value, exists := evidence.pod.Labels[key]; exists {
			binding.Labels[key] = value
		} else {
			delete(binding.Labels, key)
		}
	}
	binding.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] = evidence.pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation]
	binding.Annotations[NodeUIDAnnotation] = string(evidence.node.UID)
	binding.Annotations[NodeBootIDAnnotation] = evidence.node.Status.NodeInfo.BootID
	marshaled, err := json.Marshal(binding)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("encode attested Pod binding: %w", err))
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaled)
}

func (v *BindingValidator) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Create || request.SubResource != "binding" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL Pod binding validation request %s %q", request.Operation, request.SubResource))
	}
	binding := &corev1.Binding{}
	if err := v.decoder.Decode(request, binding); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode final Pod binding: %w", err))
	}
	evidence, response := readBindingEvidence(ctx, v.reader, request, binding)
	if response != nil {
		return *response
	}
	if response := validateBindingMetadata(binding, evidence); response != nil {
		return *response
	}
	if response := validateBoundPodContract(ctx, v.reader, evidence.pod, evidence.node, evidence.cluster); response != nil {
		return *response
	}
	return admission.Allowed("final Pod binding preserves the PostgreSQL Pod and Node identities")
}

// validateBindingMetadata enforces that the final Binding's copied metadata is an
// exact sanctioned allowlist: every label is either a protected Pod label (equal
// to the Pod's) or a Node-derived topology label (equal to the Node's), and every
// annotation is the Pod's cluster-UID or one of the Node's incarnation
// annotations. The API server copies Binding annotations — and, with the
// PodTopologyLabels admission plugin, Binding labels — onto the Pod, so any other
// entry would silently overwrite the Pod's validated contract metadata after
// CREATE-time validation. Anything outside the allowlist, or a value that does
// not match the authoritative Pod/Node, is rejected.
func validateBindingMetadata(binding *corev1.Binding, evidence *bindingEvidence) *admission.Response {
	protected := protectedBindingLabels()
	protectedSet := make(map[string]struct{}, len(protected))
	for _, key := range protected {
		protectedSet[key] = struct{}{}
	}
	// A Binding label the API server would copy onto the Pod is only permitted if
	// it is a protected identity label already carrying the Pod's own value, or a
	// topology label carrying the selected Node's value. A missing protected label
	// is harmless (the Pod keeps its own), so only extra or divergent labels are
	// rejected.
	for key, value := range binding.Labels {
		if key == corev1.LabelTopologyZone || key == corev1.LabelTopologyRegion {
			if value != evidence.node.Labels[key] {
				return deniedf("PostgreSQL Pod binding topology label %s does not match the selected Node", key)
			}
			continue
		}
		if _, ok := protectedSet[key]; !ok {
			return deniedf("PostgreSQL Pod binding carries an unexpected label %s", key)
		}
		podValue, podHas := evidence.pod.Labels[key]
		if !podHas || value != podValue {
			return deniedf("managed PostgreSQL Pod binding label %s does not match the selected Pod", key)
		}
	}
	for key, value := range binding.Annotations {
		switch key {
		case owned.PostgreSQLPodClusterUIDAnnotation:
			if value != evidence.pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] {
				return deniedf("managed PostgreSQL Pod binding does not carry the selected Pod cluster identity")
			}
		case NodeUIDAnnotation:
			if value != string(evidence.node.UID) {
				return deniedf("managed PostgreSQL Pod binding does not carry the selected Node incarnation")
			}
		case NodeBootIDAnnotation:
			if value != evidence.node.Status.NodeInfo.BootID {
				return deniedf("managed PostgreSQL Pod binding does not carry the selected Node incarnation")
			}
		default:
			return deniedf("PostgreSQL Pod binding carries an unexpected annotation %s", key)
		}
	}
	return nil
}

func readBindingEvidence(ctx context.Context, reader client.Reader, request admission.Request, binding *corev1.Binding) (*bindingEvidence, *admission.Response) {
	if binding.Namespace != request.Namespace || binding.Name != request.Name {
		response := admission.Denied("Pod binding identity does not match the admission request path")
		return nil, &response
	}
	pod := &corev1.Pod{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: request.Namespace, Name: request.Name}, pod); err != nil {
		response := admission.Errored(http.StatusInternalServerError, fmt.Errorf("read Pod selected for binding: %w", err))
		return nil, &response
	}
	kind, _, _, _ := classifyContractPod(pod)
	switch kind {
	case contractPodUnmanaged:
		if isManagedLooking(pod) {
			response := admission.Denied("managed-looking PostgreSQL Pod carries a malformed identity")
			return nil, &response
		}
		response := admission.Allowed("Pod is not a managed PostgreSQL member")
		return nil, &response
	case contractPodMember:
		// Members carry the termination fence and replication-role shape;
		// supporting pods (pooler/orchestrator) validate the stamped contract
		// without those member-only requirements.
		if !IsManagedPostgreSQLPod(pod) {
			response := admission.Denied("managed PostgreSQL Pod has incomplete identity or no termination fence")
			return nil, &response
		}
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" {
		response := admission.Denied("managed PostgreSQL Pod must be live and unassigned when its node identity is bound")
		return nil, &response
	}
	if binding.UID == "" || binding.UID != pod.UID {
		response := admission.Denied("managed PostgreSQL Pod binding must carry the exact Pod UID")
		return nil, &response
	}
	cluster, response := validateManagedPodClusterContract(ctx, reader, pod)
	if response != nil {
		return nil, response
	}
	if binding.Target.Kind != "Node" || binding.Target.Name == "" {
		response := admission.Denied("managed PostgreSQL Pod binding must select a named Node")
		return nil, &response
	}
	node := &corev1.Node{}
	if err := reader.Get(ctx, types.NamespacedName{Name: binding.Target.Name}, node); err != nil {
		response := admission.Errored(http.StatusInternalServerError, fmt.Errorf("read Node selected for PostgreSQL Pod: %w", err))
		return nil, &response
	}
	if node.UID == "" || node.Status.NodeInfo.BootID == "" {
		response := admission.Denied("selected Node has no stable UID and boot ID")
		return nil, &response
	}
	return &bindingEvidence{pod: pod, node: node, cluster: cluster}, nil
}

// validateManagedPodClusterContract binds a managed PostgreSQL Pod to its live
// owning PgShardCluster and enforces the cluster-aware replication contracts.
// It runs at Pod CREATE and again at binding admission. The replication-mode
// check is deliberately independent of the role-neutral classifiers: any pod
// that even hints at a replication process composition must validate as an
// exact policy-bound source or standby, so a serving-role label or a mangled
// composition can never dodge transport validation.
func validateManagedPodClusterContract(ctx context.Context, reader client.Reader, pod *corev1.Pod) (*pgshardv1alpha1.PgShardCluster, *admission.Response) {
	clusterName := pod.Labels[owned.ClusterLabel]
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: clusterName}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			response := admission.Denied("managed PostgreSQL Pod's owning PgShardCluster no longer exists")
			return nil, &response
		}
		response := admission.Errored(http.StatusInternalServerError, fmt.Errorf("read PgShardCluster selected for PostgreSQL Pod admission: %w", err))
		return nil, &response
	}
	if cluster.UID == "" || cluster.UID != types.UID(pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation]) {
		response := admission.Denied("managed PostgreSQL Pod does not belong to the live PgShardCluster UID")
		return nil, &response
	}
	if cluster.DeletionTimestamp != nil {
		response := admission.Denied("managed PostgreSQL Pod cannot be admitted while its PgShardCluster is deleting")
		return nil, &response
	}
	if owned.IsPostgreSQLReplicationBootstrapSourcePod(pod) {
		if !owned.IsCurrentPostgreSQLReplicationBootstrapSourcePod(pod) {
			response := admission.Denied("legacy PostgreSQL replication-bootstrap source cannot receive a new Pod admission")
			return nil, &response
		}
		if _, err := owned.ObservePostgreSQLRuntimeForCluster(cluster, pod.Labels, pod.Annotations, pod.Spec); err != nil {
			response := admission.Denied(fmt.Sprintf("PostgreSQL replication-bootstrap source does not match its PgShardCluster generation contract: %v", err))
			return nil, &response
		}
	}
	if owned.IsPostgreSQLReplicationStandbyPod(pod) {
		if _, err := owned.ObservePostgreSQLRuntimeForCluster(cluster, pod.Labels, pod.Annotations, pod.Spec); err != nil {
			response := admission.Denied(fmt.Sprintf("PostgreSQL replication standby does not match its PgShardCluster transport contract: %v", err))
			return nil, &response
		}
	}
	if owned.PostgreSQLReplicationModeEnvironmentPresent(pod.Spec) {
		if _, hasRole := pod.Labels[owned.RoleLabel]; hasRole {
			response := admission.Denied("managed PostgreSQL replication source and standby Pods must not carry a serving role")
			return nil, &response
		}
		if err := owned.ValidatePostgreSQLReplicationPodContract(cluster, pod.Labels, pod.Annotations, pod.Spec); err != nil {
			response := admission.Denied(fmt.Sprintf("managed PostgreSQL replication Pod does not match its PgShardCluster transport contract: %v", err))
			return nil, &response
		}
	}
	return cluster, nil
}

// PodCreateValidator refuses managed PostgreSQL Pods that try to enter the
// namespace outside the operator's contract: pre-assigned to a node (which
// would skip pods/binding admission entirely), carrying forged binding
// evidence, or composed as a replication member that does not match the
// owning PgShardCluster's recorded transport policy and TLS checkpoints.
// Pods that are not managed PostgreSQL members are deliberately allowed.
type PodCreateValidator struct {
	reader     client.Reader
	identities ControllerIdentities
	decoder    admission.Decoder
}

func NewPodCreateValidator(reader client.Reader, identities ControllerIdentities, scheme *runtime.Scheme) *PodCreateValidator {
	return &PodCreateValidator{reader: reader, identities: identities, decoder: admission.NewDecoder(scheme)}
}

func (v *PodCreateValidator) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Create || request.SubResource != "" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL Pod create validation request %s %q", request.Operation, request.SubResource))
	}
	pod := &corev1.Pod{}
	if err := v.decoder.Decode(request, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode created Pod: %w", err))
	}
	kind, shard, member, clusterName := classifyContractPod(pod)
	switch kind {
	case contractPodUnmanaged:
		if isPotentialManagedPostgreSQLPod(pod) {
			return admission.Denied("managed PostgreSQL Pod has incomplete identity or an unrecognized composition")
		}
		return admission.Allowed("Pod is not a managed PostgreSQL member")
	case contractPodMember:
		if !IsManagedPostgreSQLPod(pod) {
			return admission.Denied("managed PostgreSQL Pod has incomplete identity or no termination fence")
		}
		if pod.Spec.NodeName != "" || pod.Annotations[NodeUIDAnnotation] != "" || pod.Annotations[NodeBootIDAnnotation] != "" {
			return admission.Denied("managed PostgreSQL Pod must be created unassigned and scheduled through binding")
		}
		if _, response := validateManagedPodClusterContract(ctx, v.reader, pod); response != nil {
			return *response
		}
	}
	// The canonical contract is enforced whenever the reconciler's stamp is
	// present; requiring the stamp on every managed pod is the activation
	// stage's job (deferred), so a stampless pod keeps the legacy behavior.
	if pod.Annotations[owned.PodContractHashAnnotation] == "" {
		return admission.Allowed("managed PostgreSQL Pod creation matches its PgShardCluster contract")
	}
	if err := decodeStrictObject(request.Object.Raw, &corev1.Pod{}); err != nil {
		return admission.Denied(fmt.Sprintf("managed Pod carries unknown or duplicate fields: %v", err))
	}
	if pod.Namespace != "" && pod.Namespace != request.Namespace {
		return admission.Denied("managed Pod namespace does not match the request namespace")
	}
	pod.Namespace = request.Namespace
	if response := v.validatePodContract(ctx, request, pod, kind, shard, member, clusterName); response != nil {
		return *response
	}
	return admission.Allowed("managed PostgreSQL Pod creation matches its PgShardCluster contract")
}

func protectedBindingLabels() [6]string {
	return [6]string{owned.ManagedByLabel, owned.ComponentLabel, owned.ClusterLabel, owned.ShardLabel, owned.RoleLabel, owned.MemberLabel}
}

func isPotentialManagedPostgreSQLPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	postgresLabels := pod.Labels[owned.ManagedByLabel] == owned.ManagedByValue && pod.Labels[owned.ComponentLabel] == "postgresql"
	memberLabels := pod.Labels[owned.ClusterLabel] != "" && pod.Labels[owned.ShardLabel] != "" &&
		pod.Labels[owned.RoleLabel] != "" && pod.Labels[owned.MemberLabel] != ""
	return postgresLabels || memberLabels ||
		pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] != "" ||
		pod.Annotations[NodeUIDAnnotation] != "" || pod.Annotations[NodeBootIDAnnotation] != "" ||
		slices.Contains(pod.Finalizers, owned.PostgreSQLPodTerminationFinalizer)
}

type StatusAttestor struct {
	reader  client.Reader
	decoder admission.Decoder
	codec   *HandshakeCodec
}

func NewStatusAttestor(reader client.Reader, codec *HandshakeCodec, scheme *runtime.Scheme) *StatusAttestor {
	return &StatusAttestor{reader: reader, decoder: admission.NewDecoder(scheme), codec: codec}
}

func (a *StatusAttestor) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Update || request.SubResource != "status" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL Pod status request %s %q", request.Operation, request.SubResource))
	}
	oldPod, newPod, response := a.decodeUpdate(request)
	if response != nil {
		return *response
	}
	if response := validateManagedStatusIdentity(oldPod, newPod); response != nil {
		return *response
	}
	_, oldCount := terminationAttestation(oldPod)
	_, newCount := terminationAttestation(newPod)
	if newCount > 1 {
		return admission.Denied("managed PostgreSQL Pod has duplicate process-termination attestations")
	}
	mutated := oldCount == 0 && hasTerminalPhase(newPod)
	if mutated {
		if newPod.Spec.NodeName == "" || newPod.Annotations[NodeUIDAnnotation] == "" || newPod.Annotations[NodeBootIDAnnotation] == "" {
			return admission.Denied("managed PostgreSQL Pod has no binding-time node identity")
		}
		receipt, err := a.codec.TerminationReceipt(ctx, newPod)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("authenticate process-termination attestation: %w", err))
		}
		conditions := make([]corev1.PodCondition, 0, len(newPod.Status.Conditions)+1)
		for _, condition := range newPod.Status.Conditions {
			if condition.Type != TerminationConditionType {
				conditions = append(conditions, condition)
			}
		}
		conditions = append(conditions, NewTerminationAttestation(newPod, metav1.Now(), receipt))
		newPod.Status.Conditions = conditions
	}
	validated := a.validateFinalStatus(ctx, request, oldPod, newPod)
	if !validated.Allowed || !mutated {
		return validated
	}
	marshaled, err := json.Marshal(newPod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("encode attested Pod status: %w", err))
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaled)
}

func (a *StatusAttestor) decodeUpdate(request admission.Request) (*corev1.Pod, *corev1.Pod, *admission.Response) {
	oldPod := &corev1.Pod{}
	if err := a.decoder.DecodeRaw(request.OldObject, oldPod); err != nil {
		response := admission.Errored(http.StatusBadRequest, fmt.Errorf("decode old Pod status: %w", err))
		return nil, nil, &response
	}
	newPod := &corev1.Pod{}
	if err := a.decoder.Decode(request, newPod); err != nil {
		response := admission.Errored(http.StatusBadRequest, fmt.Errorf("decode new Pod status: %w", err))
		return nil, nil, &response
	}
	return oldPod, newPod, nil
}

func (a *StatusAttestor) validateKubelet(ctx context.Context, request admission.Request, pod *corev1.Pod) error {
	if pod.Spec.NodeName == "" || pod.Annotations[NodeUIDAnnotation] == "" || pod.Annotations[NodeBootIDAnnotation] == "" {
		return fmt.Errorf("managed PostgreSQL Pod has no binding-time node identity")
	}
	if request.UserInfo.Username != "system:node:"+pod.Spec.NodeName || !slices.Contains(request.UserInfo.Groups, "system:nodes") {
		return fmt.Errorf("terminal phase was not reported by the authenticated kubelet for Node %s", pod.Spec.NodeName)
	}
	node := &corev1.Node{}
	if err := a.reader.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("bound Node %s no longer exists", pod.Spec.NodeName)
		}
		return fmt.Errorf("read bound Node %s: %w", pod.Spec.NodeName, err)
	}
	if string(node.UID) != pod.Annotations[NodeUIDAnnotation] || node.Status.NodeInfo.BootID != pod.Annotations[NodeBootIDAnnotation] {
		return fmt.Errorf("bound Node %s is not the Pod's binding-time node incarnation", pod.Spec.NodeName)
	}
	return validateStoppedContainers(pod)
}

func validateStoppedContainers(pod *corev1.Pod) error {
	for _, group := range []struct {
		kind     string
		names    []string
		statuses []corev1.ContainerStatus
	}{
		{kind: "init", names: containerNames(pod.Spec.InitContainers), statuses: pod.Status.InitContainerStatuses},
		{kind: "application", names: containerNames(pod.Spec.Containers), statuses: pod.Status.ContainerStatuses},
		{kind: "ephemeral", names: ephemeralContainerNames(pod.Spec.EphemeralContainers), statuses: pod.Status.EphemeralContainerStatuses},
	} {
		if err := validateStoppedContainerStatuses(group.kind, group.names, group.statuses); err != nil {
			return err
		}
	}
	return nil
}

type StatusValidator struct {
	attestor *StatusAttestor
}

func NewStatusValidator(reader client.Reader, codec *HandshakeCodec, scheme *runtime.Scheme) *StatusValidator {
	return &StatusValidator{attestor: NewStatusAttestor(reader, codec, scheme)}
}

func (v *StatusValidator) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Update || request.SubResource != "status" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL Pod status validation request %s %q", request.Operation, request.SubResource))
	}
	oldPod, newPod, response := v.attestor.decodeUpdate(request)
	if response != nil {
		return *response
	}
	return v.attestor.validateFinalStatus(ctx, request, oldPod, newPod)
}

func (a *StatusAttestor) validateFinalStatus(ctx context.Context, request admission.Request, oldPod, newPod *corev1.Pod) admission.Response {
	if response := validateManagedStatusIdentity(oldPod, newPod); response != nil {
		return *response
	}
	oldAttestation, oldCount := terminationAttestation(oldPod)
	newAttestation, newCount := terminationAttestation(newPod)
	if oldCount > 1 || newCount > 1 {
		return admission.Denied("managed PostgreSQL Pod has duplicate process-termination attestations")
	}
	if hasTerminalPhase(oldPod) && !hasTerminalPhase(newPod) {
		return admission.Denied("managed PostgreSQL Pod terminal phase is immutable")
	}
	if oldCount != 0 {
		if oldCount != newCount || !reflect.DeepEqual(oldAttestation, newAttestation) {
			return admission.Denied("process-termination attestation is immutable")
		}
		if err := validateStoppedContainers(newPod); err != nil {
			return admission.Denied(err.Error())
		}
		verified, err := a.codec.VerifyTermination(ctx, newPod)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("verify final process-termination attestation: %w", err))
		}
		if !verified {
			return admission.Denied("process-termination attestation is not authenticated")
		}
		return admission.Allowed("final status preserves authenticated termination evidence")
	}
	if !hasTerminalPhase(newPod) {
		if newCount != 0 {
			return admission.Denied("a nonterminal PostgreSQL Pod cannot carry process-termination evidence")
		}
		return admission.Allowed("final status is nonterminal and preserves the lifecycle fence")
	}
	if newCount != 1 || !HasTerminationAttestation(newPod) {
		return admission.Denied("terminal PostgreSQL Pod lacks the authenticated process-termination attestation")
	}
	if err := a.validateKubelet(ctx, request, newPod); err != nil {
		return admission.Denied(err.Error())
	}
	verified, err := a.codec.VerifyTermination(ctx, newPod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("verify final process-termination attestation: %w", err))
	}
	if !verified {
		return admission.Denied("process-termination attestation is not authenticated")
	}
	return admission.Allowed("final status contains authenticated termination evidence")
}

func validateManagedStatusIdentity(oldPod, newPod *corev1.Pod) *admission.Response {
	oldManaged := IsManagedPostgreSQLPod(oldPod)
	newManaged := IsManagedPostgreSQLPod(newPod)
	if !oldManaged && !newManaged {
		response := admission.Allowed("Pod is not a managed PostgreSQL member")
		return &response
	}
	if !oldManaged || !newManaged || !managedIdentityEqual(oldPod, newPod) {
		response := admission.Denied("managed PostgreSQL Pod identity changed during a status update")
		return &response
	}
	return nil
}

func validateStoppedContainerStatuses(kind string, names []string, statuses []corev1.ContainerStatus) error {
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}
	observed := make(map[string]struct{}, len(statuses))
	for _, status := range statuses {
		if _, exists := wanted[status.Name]; !exists {
			return fmt.Errorf("terminal phase reports unknown %s container %s", kind, status.Name)
		}
		if _, duplicate := observed[status.Name]; duplicate {
			return fmt.Errorf("terminal phase reports duplicate %s container %s", kind, status.Name)
		}
		observed[status.Name] = struct{}{}
		states := 0
		if status.State.Waiting != nil {
			states++
		}
		if status.State.Running != nil {
			states++
		}
		if status.State.Terminated != nil {
			states++
		}
		if states != 1 {
			return fmt.Errorf("terminal phase has ambiguous %s container state for %s", kind, status.Name)
		}
		if status.State.Running != nil {
			return fmt.Errorf("terminal phase still reports %s container %s running", kind, status.Name)
		}
	}
	for _, name := range names {
		if _, exists := observed[name]; !exists {
			return fmt.Errorf("terminal phase omits %s container status for %s", kind, name)
		}
	}
	return nil
}

func containerNames(containers []corev1.Container) []string {
	names := make([]string, 0, len(containers))
	for _, container := range containers {
		names = append(names, container.Name)
	}
	return names
}

func ephemeralContainerNames(containers []corev1.EphemeralContainer) []string {
	names := make([]string, 0, len(containers))
	for _, container := range containers {
		names = append(names, container.Name)
	}
	return names
}

type MetadataValidator struct {
	decoder admission.Decoder
	codec   *HandshakeCodec
}

type NamespaceValidator struct {
	decoder admission.Decoder
}

func NewNamespaceValidator(scheme *runtime.Scheme) *NamespaceValidator {
	return &NamespaceValidator{decoder: admission.NewDecoder(scheme)}
}

func (v *NamespaceValidator) Handle(_ context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Update ||
		request.SubResource != "" && request.SubResource != "status" && request.SubResource != "finalize" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL fencing namespace request %s %q", request.Operation, request.SubResource))
	}
	oldNamespace := &corev1.Namespace{}
	if err := v.decoder.DecodeRaw(request.OldObject, oldNamespace); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode old PostgreSQL fencing namespace: %w", err))
	}
	newNamespace := &corev1.Namespace{}
	if err := v.decoder.Decode(request, newNamespace); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode new PostgreSQL fencing namespace: %w", err))
	}
	if oldNamespace.Labels[NamespaceLabel] != NamespaceLabelValue {
		return admission.Allowed("namespace has not enabled PostgreSQL Pod fencing")
	}
	if newNamespace.Labels[NamespaceLabel] != NamespaceLabelValue {
		return admission.Denied(fmt.Sprintf("namespace label %s=%s is immutable once PostgreSQL Pod fencing is enabled", NamespaceLabel, NamespaceLabelValue))
	}
	return admission.Allowed("namespace preserves PostgreSQL Pod fencing")
}

func NewMetadataValidator(codec *HandshakeCodec, scheme *runtime.Scheme) *MetadataValidator {
	return &MetadataValidator{decoder: admission.NewDecoder(scheme), codec: codec}
}

func (v *MetadataValidator) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Update || request.SubResource != "" && request.SubResource != "ephemeralcontainers" && request.SubResource != "resize" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL Pod metadata request %s %q", request.Operation, request.SubResource))
	}
	oldPod := &corev1.Pod{}
	if err := v.decoder.DecodeRaw(request.OldObject, oldPod); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode old Pod: %w", err))
	}
	newPod := &corev1.Pod{}
	if err := v.decoder.Decode(request, newPod); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode new Pod: %w", err))
	}
	oldKind, _, _, _ := classifyContractPod(oldPod)
	newKind, _, _, _ := classifyContractPod(newPod)
	oldLooking := isManagedLooking(oldPod)
	newLooking := isManagedLooking(newPod)
	// A managed-looking pod whose shard/member is noncanonical is malformed and
	// may never be admitted through UPDATE (it would read as managed to the
	// fencing logic while dodging the canonical contract).
	if newLooking && newKind == contractPodUnmanaged {
		return admission.Denied("managed-looking PostgreSQL Pod carries a malformed identity")
	}
	// ADOPTION: an unmanaged pod may never gain a managed identity.
	if !oldLooking && newLooking {
		return admission.Denied("unmanaged PostgreSQL Pod may not be mutated into a managed identity")
	}
	if !oldLooking {
		return admission.Allowed("Pod is not a managed PostgreSQL member")
	}
	// A complete member keeps its termination-fence lifecycle; every other
	// protected pod (supporting, or a malformed-old identity) is fully immutable.
	if oldKind == contractPodMember && IsManagedPostgreSQLPod(oldPod) {
		return v.validateManagedMemberUpdate(ctx, oldPod, newPod)
	}
	return validateManagedPodUpdate(oldPod, newPod)
}

// validateManagedMemberUpdate holds a complete member pod's full metadata and
// spec immutable, permitting only the authenticated termination-finalizer
// removal during deletion.
func (v *MetadataValidator) validateManagedMemberUpdate(ctx context.Context, oldPod, newPod *corev1.Pod) admission.Response {
	if response := protectedPodMetadataImmutable(oldPod, newPod); response != nil {
		return *response
	}
	if oldPod.Generation != newPod.Generation || !reflect.DeepEqual(oldPod.Spec, newPod.Spec) {
		return admission.Denied("managed PostgreSQL Pod spec and generation are immutable")
	}
	if !finalizersImmutableExceptTermination(oldPod, newPod) {
		return admission.Denied("managed PostgreSQL Pod finalizers are immutable except the termination fence")
	}
	oldFinalizer := slices.Contains(oldPod.Finalizers, owned.PostgreSQLPodTerminationFinalizer)
	newFinalizer := slices.Contains(newPod.Finalizers, owned.PostgreSQLPodTerminationFinalizer)
	if oldFinalizer == newFinalizer {
		return admission.Allowed("managed PostgreSQL Pod termination fence is unchanged")
	}
	if !oldFinalizer || oldPod.DeletionTimestamp == nil {
		return admission.Denied("managed PostgreSQL Pod termination fence may only be removed during deletion")
	}
	if oldPod.Spec.NodeName != "" {
		verified, err := v.codec.VerifyTermination(ctx, oldPod)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("verify process-stop evidence before finalizer removal: %w", err))
		}
		if !verified {
			return admission.Denied("managed PostgreSQL Pod termination fence requires authenticated process-stop evidence")
		}
	}
	return admission.Allowed("managed PostgreSQL Pod termination fence has safe release evidence")
}

func IsManagedPostgreSQLPod(pod *corev1.Pod) bool {
	return pod != nil &&
		pod.Labels[owned.ManagedByLabel] == owned.ManagedByValue &&
		pod.Labels[owned.ComponentLabel] == "postgresql" &&
		pod.Labels[owned.ClusterLabel] != "" &&
		pod.Labels[owned.ShardLabel] != "" &&
		(pod.Labels[owned.RoleLabel] != "" ||
			owned.IsPostgreSQLReplicationBootstrapSourcePod(pod) ||
			owned.IsPostgreSQLReplicationStandbyPod(pod)) &&
		pod.Labels[owned.MemberLabel] != "" &&
		pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] != "" &&
		slices.Contains(pod.Finalizers, owned.PostgreSQLPodTerminationFinalizer)
}

func HasTerminationAttestation(pod *corev1.Pod) bool {
	condition, count := terminationAttestation(pod)
	receipt := ""
	if condition != nil {
		receipt = terminationReceiptFromMessage(condition.Message)
	}
	return count == 1 && hasTerminalPhase(pod) &&
		pod.Annotations[NodeUIDAnnotation] != "" && pod.Annotations[NodeBootIDAnnotation] != "" &&
		condition.Status == corev1.ConditionTrue && condition.ObservedGeneration == pod.Generation &&
		condition.Reason == TerminationReason && receipt != "" && condition.Message == nodeIdentityMessage(pod)+";receipt="+receipt
}

func NewTerminationAttestation(pod *corev1.Pod, transitionTime metav1.Time, receipt string) corev1.PodCondition {
	return corev1.PodCondition{
		Type:               TerminationConditionType,
		Status:             corev1.ConditionTrue,
		ObservedGeneration: pod.Generation,
		LastTransitionTime: transitionTime,
		Reason:             TerminationReason,
		Message:            nodeIdentityMessage(pod) + ";receipt=" + receipt,
	}
}

func terminationAttestation(pod *corev1.Pod) (*corev1.PodCondition, int) {
	var found *corev1.PodCondition
	count := 0
	for index := range pod.Status.Conditions {
		if pod.Status.Conditions[index].Type != TerminationConditionType {
			continue
		}
		count++
		found = &pod.Status.Conditions[index]
	}
	return found, count
}

func managedIdentityEqual(oldPod, newPod *corev1.Pod) bool {
	if oldPod.UID != newPod.UID || oldPod.Spec.NodeName != newPod.Spec.NodeName {
		return false
	}
	for _, key := range []string{owned.ManagedByLabel, owned.ComponentLabel, owned.ClusterLabel, owned.ShardLabel, owned.RoleLabel, owned.MemberLabel} {
		oldValue, oldHas := oldPod.Labels[key]
		newValue, newHas := newPod.Labels[key]
		if oldHas != newHas || oldValue != newValue {
			return false
		}
	}
	for _, key := range []string{
		owned.PostgreSQLPodClusterUIDAnnotation,
		owned.PostgreSQLRuntimeAnnotation,
		owned.PostgreSQLGenerationDurabilityAnnotation,
		owned.PostgreSQLSynchronousStandbysAnnotation,
		owned.PodContractHashAnnotation,
		owned.PodSecurityGenerationAnnotation,
		NodeUIDAnnotation,
		NodeBootIDAnnotation,
	} {
		oldValue, oldHas := oldPod.Annotations[key]
		newValue, newHas := newPod.Annotations[key]
		if oldHas != newHas || oldValue != newValue {
			return false
		}
	}
	return controllerOwnerReferenceEqual(oldPod.OwnerReferences, newPod.OwnerReferences)
}

// controllerOwnerReferenceEqual reports whether two objects carry the same
// controller owner reference (or both none). The controller owner reference is
// the pod's authoring-provenance anchor and is immutable post-create.
func controllerOwnerReferenceEqual(old, updated []metav1.OwnerReference) bool {
	oldRef, newRef := controllerOwnerRef(old), controllerOwnerRef(updated)
	if (oldRef == nil) != (newRef == nil) {
		return false
	}
	return oldRef == nil || (oldRef.Kind == newRef.Kind && oldRef.Name == newRef.Name && oldRef.UID == newRef.UID)
}

// validateManagedPodUpdate holds a protected non-member pod (supporting, or a
// malformed-old managed identity) fully immutable: it denies escape, any
// label/annotation/ownerReference mutation, ephemeral containers, any spec
// mutation (covering a diverging resize), and any finalizer change.
func validateManagedPodUpdate(oldPod, newPod *corev1.Pod) admission.Response {
	if !isManagedLooking(newPod) {
		return admission.Denied("managed PostgreSQL Pod may not shed its managed identity")
	}
	if response := protectedPodMetadataImmutable(oldPod, newPod); response != nil {
		return *response
	}
	if len(newPod.Spec.EphemeralContainers) != 0 {
		return admission.Denied("managed PostgreSQL Pod must not carry ephemeral containers")
	}
	if !equality.Semantic.DeepEqual(oldPod.Spec, newPod.Spec) {
		return admission.Denied("managed PostgreSQL Pod spec is immutable")
	}
	if !equalStringSets(oldPod.Finalizers, newPod.Finalizers) {
		return admission.Denied("managed PostgreSQL Pod finalizers are immutable")
	}
	return admission.Allowed("managed PostgreSQL Pod update preserves its contract")
}

// protectedPodMetadataImmutable requires a protected pod's identity anchors —
// UID, node assignment, the complete label and annotation sets, and every owner
// reference — to be byte-for-byte unchanged across an UPDATE. Only server fields
// outside metadata identity (resourceVersion, managedFields, the deletion
// timestamp) and the separately governed finalizers/spec may differ.
func protectedPodMetadataImmutable(oldPod, newPod *corev1.Pod) *admission.Response {
	if oldPod.UID != newPod.UID || oldPod.Spec.NodeName != newPod.Spec.NodeName ||
		!stringMapsEqual(oldPod.Labels, newPod.Labels) ||
		!stringMapsEqual(oldPod.Annotations, newPod.Annotations) ||
		!equality.Semantic.DeepEqual(oldPod.OwnerReferences, newPod.OwnerReferences) {
		return deniedf("managed PostgreSQL Pod identity is immutable")
	}
	return nil
}

func finalizersImmutableExceptTermination(oldPod, newPod *corev1.Pod) bool {
	return equalStringSets(
		withoutString(oldPod.Finalizers, owned.PostgreSQLPodTerminationFinalizer),
		withoutString(newPod.Finalizers, owned.PostgreSQLPodTerminationFinalizer),
	)
}

func withoutString(values []string, drop string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != drop {
			out = append(out, value)
		}
	}
	return out
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, value := range a {
		counts[value]++
	}
	for _, value := range b {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	return true
}

func stringMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func nodeIdentityMessage(pod *corev1.Pod) string {
	return fmt.Sprintf("nodeUID=%s;bootID=%s", pod.Annotations[NodeUIDAnnotation], pod.Annotations[NodeBootIDAnnotation])
}

func terminationReceiptFromMessage(message string) string {
	_, receipt, found := strings.Cut(message, ";receipt=")
	if !found || strings.Contains(receipt, ";") {
		return ""
	}
	return receipt
}

func hasTerminalPhase(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// +kubebuilder:webhook:path=/mutate-core-v1-postgresqlpodbinding,mutating=true,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/binding,verbs=create,versions=v1,name=mpostgresqlpodbinding.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-core-v1-postgresqlpodbinding,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/binding,verbs=create,versions=v1,name=vpostgresqlpodbinding.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/mutate-core-v1-postgresqlpodstatus,mutating=true,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/status,verbs=update,versions=v1,name=mpostgresqlpodstatus.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/mutate-pgshard-io-v1alpha1-postgresqlfencinghandshake,mutating=true,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups=pgshard.io,resources=pgshardclusters,verbs=update,versions=v1alpha1,name=mpostgresqlfencinghandshake.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-core-v1-postgresqlpodcreate,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods,verbs=create,versions=v1,name=vpostgresqlpodcreate.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-core-v1-postgresqlpodmetadata,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods;pods/ephemeralcontainers;pods/resize,verbs=update,versions=v1,name=vpostgresqlpodmetadata.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-core-v1-postgresqlpodstatus,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/status,verbs=update,versions=v1,name=vpostgresqlpodstatus.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-core-v1-postgresqlfencingnamespace,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=namespaces;namespaces/status;namespaces/finalize,verbs=update,versions=v1,name=vpostgresqlfencingnamespace.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-apps-v1-postgresqlworkload,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups=apps,resources=statefulsets;deployments;replicasets;statefulsets/scale;deployments/scale;replicasets/scale,verbs=create;update,versions=v1,name=vpostgresqlworkload.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-core-v1-postgresqlpodconnect,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/exec;pods/attach;pods/portforward;pods/proxy,verbs=connect,versions=v1,name=vpostgresqlpodconnect.pgshard.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-core-v1-postgresqlpodconnect,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/exec;pods/attach;pods/portforward;pods/proxy,verbs=connect,versions=v1,name=vpostgresqlmanagerconnect.pgshard.io,admissionReviewVersions=v1,servicePort=9444
