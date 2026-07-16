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

	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
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

	NodeUIDAnnotation    = "pgshard.io/postgresql-node-uid"
	NodeBootIDAnnotation = "pgshard.io/postgresql-node-boot-id"

	TerminationConditionType corev1.PodConditionType = "pgshard.io/PostgreSQLProcessTerminated"
	TerminationReason                                = "AuthenticatedKubelet"

	BindingWebhookName  = "mpostgresqlpodbinding.pgshard.io"
	BindingWebhookPath  = "/mutate-core-v1-postgresqlpodbinding"
	StatusWebhookName   = "mpostgresqlpodstatus.pgshard.io"
	StatusWebhookPath   = "/mutate-core-v1-postgresqlpodstatus"
	MetadataWebhookName = "vpostgresqlpodmetadata.pgshard.io"
	MetadataWebhookPath = "/validate-core-v1-postgresqlpodmetadata"
)

type BindingAttestor struct {
	reader  client.Reader
	decoder admission.Decoder
}

func NewBindingAttestor(reader client.Reader, scheme *runtime.Scheme) *BindingAttestor {
	return &BindingAttestor{reader: reader, decoder: admission.NewDecoder(scheme)}
}

func (a *BindingAttestor) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Create || request.SubResource != "binding" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL Pod binding request %s %q", request.Operation, request.SubResource))
	}
	binding := &corev1.Binding{}
	if err := a.decoder.Decode(request, binding); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode Pod binding: %w", err))
	}
	pod := &corev1.Pod{}
	if err := a.reader.Get(ctx, types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}, pod); err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("read Pod selected for binding: %w", err))
	}
	if !IsManagedPostgreSQLPod(pod) {
		return admission.Allowed("Pod is not a managed PostgreSQL member")
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" {
		return admission.Denied("managed PostgreSQL Pod must be live and unassigned when its node identity is bound")
	}
	if binding.UID == "" || binding.UID != pod.UID {
		return admission.Denied("managed PostgreSQL Pod binding must carry the exact Pod UID")
	}
	if binding.Target.Kind != "Node" || binding.Target.Name == "" {
		return admission.Denied("managed PostgreSQL Pod binding must select a named Node")
	}
	node := &corev1.Node{}
	if err := a.reader.Get(ctx, types.NamespacedName{Name: binding.Target.Name}, node); err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("read Node selected for PostgreSQL Pod: %w", err))
	}
	if node.UID == "" || node.Status.NodeInfo.BootID == "" {
		return admission.Denied("selected Node has no stable UID and boot ID")
	}
	if binding.Annotations == nil {
		binding.Annotations = make(map[string]string, 2)
	}
	binding.Annotations[NodeUIDAnnotation] = string(node.UID)
	binding.Annotations[NodeBootIDAnnotation] = node.Status.NodeInfo.BootID
	marshaled, err := json.Marshal(binding)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("encode attested Pod binding: %w", err))
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaled)
}

type StatusAttestor struct {
	reader  client.Reader
	decoder admission.Decoder
}

func NewStatusAttestor(reader client.Reader, scheme *runtime.Scheme) *StatusAttestor {
	return &StatusAttestor{reader: reader, decoder: admission.NewDecoder(scheme)}
}

func (a *StatusAttestor) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Update || request.SubResource != "status" {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected PostgreSQL Pod status request %s %q", request.Operation, request.SubResource))
	}
	oldPod, newPod, response := a.decodeUpdate(request)
	if response != nil {
		return *response
	}
	if !IsManagedPostgreSQLPod(newPod) {
		return admission.Allowed("Pod is not a managed PostgreSQL member")
	}
	if !IsManagedPostgreSQLPod(oldPod) || !managedIdentityEqual(oldPod, newPod) {
		return admission.Denied("managed PostgreSQL Pod identity changed during a status update")
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
		return admission.Allowed("status update preserves termination evidence")
	}
	if !hasTerminalPhase(newPod) {
		if newCount != 0 {
			return admission.Denied("a nonterminal PostgreSQL Pod cannot carry process-termination evidence")
		}
		return admission.Allowed("status update does not create termination evidence")
	}
	if err := a.validateKubelet(ctx, request, newPod); err != nil {
		return admission.Denied(err.Error())
	}
	conditions := make([]corev1.PodCondition, 0, len(newPod.Status.Conditions)+1)
	for _, condition := range newPod.Status.Conditions {
		if condition.Type != TerminationConditionType {
			conditions = append(conditions, condition)
		}
	}
	conditions = append(conditions, NewTerminationAttestation(newPod, metav1.Now()))
	newPod.Status.Conditions = conditions
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
}

func NewMetadataValidator(scheme *runtime.Scheme) *MetadataValidator {
	return &MetadataValidator{decoder: admission.NewDecoder(scheme)}
}

func (v *MetadataValidator) Handle(_ context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Update || request.SubResource != "" {
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
	if !IsManagedPostgreSQLPod(oldPod) {
		return admission.Allowed("Pod is not a managed PostgreSQL member")
	}
	if !managedIdentityEqual(oldPod, newPod) {
		return admission.Denied("managed PostgreSQL Pod identity and binding-time node identity are immutable")
	}
	oldFinalizer := slices.Contains(oldPod.Finalizers, owned.PostgreSQLPodTerminationFinalizer)
	newFinalizer := slices.Contains(newPod.Finalizers, owned.PostgreSQLPodTerminationFinalizer)
	if oldFinalizer == newFinalizer {
		return admission.Allowed("managed PostgreSQL Pod termination fence is unchanged")
	}
	if !oldFinalizer || oldPod.DeletionTimestamp == nil {
		return admission.Denied("managed PostgreSQL Pod termination fence may only be removed during deletion")
	}
	if oldPod.Spec.NodeName != "" && !HasTerminationAttestation(oldPod) {
		return admission.Denied("managed PostgreSQL Pod termination fence requires authenticated process-stop evidence")
	}
	return admission.Allowed("managed PostgreSQL Pod termination fence has safe release evidence")
}

func IsManagedPostgreSQLPod(pod *corev1.Pod) bool {
	return pod != nil &&
		pod.Labels[owned.ManagedByLabel] == owned.ManagedByValue &&
		pod.Labels[owned.ComponentLabel] == "postgresql" &&
		pod.Labels[owned.ClusterLabel] != "" &&
		pod.Labels[owned.ShardLabel] != "" &&
		pod.Labels[owned.RoleLabel] != "" &&
		pod.Labels[owned.MemberLabel] != "" &&
		pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] != "" &&
		slices.Contains(pod.Finalizers, owned.PostgreSQLPodTerminationFinalizer)
}

func HasTerminationAttestation(pod *corev1.Pod) bool {
	condition, count := terminationAttestation(pod)
	return count == 1 && hasTerminalPhase(pod) &&
		pod.Annotations[NodeUIDAnnotation] != "" && pod.Annotations[NodeBootIDAnnotation] != "" &&
		condition.Status == corev1.ConditionTrue && condition.ObservedGeneration == pod.Generation &&
		condition.Reason == TerminationReason && condition.Message == nodeIdentityMessage(pod)
}

func NewTerminationAttestation(pod *corev1.Pod, transitionTime metav1.Time) corev1.PodCondition {
	return corev1.PodCondition{
		Type:               TerminationConditionType,
		Status:             corev1.ConditionTrue,
		ObservedGeneration: pod.Generation,
		LastTransitionTime: transitionTime,
		Reason:             TerminationReason,
		Message:            nodeIdentityMessage(pod),
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
		if oldPod.Labels[key] != newPod.Labels[key] {
			return false
		}
	}
	for _, key := range []string{owned.PostgreSQLPodClusterUIDAnnotation, NodeUIDAnnotation, NodeBootIDAnnotation} {
		if oldPod.Annotations[key] != newPod.Annotations[key] {
			return false
		}
	}
	return true
}

func nodeIdentityMessage(pod *corev1.Pod) string {
	return fmt.Sprintf("nodeUID=%s;bootID=%s", pod.Annotations[NodeUIDAnnotation], pod.Annotations[NodeBootIDAnnotation])
}

func hasTerminalPhase(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// +kubebuilder:webhook:path=/mutate-core-v1-postgresqlpodbinding,mutating=true,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/binding,verbs=create,versions=v1,name=mpostgresqlpodbinding.pgshard.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/mutate-core-v1-postgresqlpodstatus,mutating=true,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/status,verbs=update,versions=v1,name=mpostgresqlpodstatus.pgshard.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-core-v1-postgresqlpodmetadata,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups="",resources=pods,verbs=update,versions=v1,name=vpostgresqlpodmetadata.pgshard.io,admissionReviewVersions=v1
