package v1alpha1

import (
	"context"
	"fmt"
	"reflect"

	authenticationv1 "k8s.io/api/authentication/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// PgShardCatalogActivationValidator owns admission-time identity separation for
// the inert carrier. RBAC in this slice grants neither future writer access.
// +kubebuilder:object:generate=false
type PgShardCatalogActivationValidator struct {
	ControllerUsername  string
	PublicationVerifier CatalogActivationPublicationVerifier
}

// CatalogActivationPublicationVerifier proves that every Kubernetes object
// bound by a proposed activation request is still the exact live publication
// observed by the publisher. Implementations must use an authoritative reader;
// the orchestrator is deliberately not granted Secret read access.
// +kubebuilder:object:generate=false
type CatalogActivationPublicationVerifier interface {
	VerifyPublication(context.Context, *PgShardCatalogActivation, *PgShardCatalogActivation) error
}

func (validator *PgShardCatalogActivationValidator) ValidateCreate(ctx context.Context, activation *PgShardCatalogActivation) (admission.Warnings, error) {
	errors := validateEmptyCatalogActivation(activation)
	request, requestErr := admission.RequestFromContext(ctx)
	if requestErr != nil {
		return nil, fmt.Errorf("read catalog activation admission identity: %w", requestErr)
	}
	if validator.ControllerUsername == "" || request.UserInfo.Username != validator.ControllerUsername {
		errors = append(errors, field.Forbidden(field.NewPath("metadata"), "only the pgshard controller may create a catalog activation carrier"))
	}
	return nil, invalidCatalogActivationIfAny(activation.Name, errors)
}

func (validator *PgShardCatalogActivationValidator) ValidateUpdate(ctx context.Context, oldActivation, newActivation *PgShardCatalogActivation) (admission.Warnings, error) {
	errors := validateCatalogActivationMetadataUpdate(oldActivation, newActivation)
	request, requestErr := admission.RequestFromContext(ctx)
	if requestErr != nil {
		return nil, fmt.Errorf("read catalog activation admission identity: %w", requestErr)
	}

	specChanged := !apiequality.Semantic.DeepEqual(oldActivation.Spec, newActivation.Spec)
	statusChanged := !apiequality.Semantic.DeepEqual(oldActivation.Status, newActivation.Status)
	if specChanged {
		errors = append(errors, validateCatalogActivationSpecTransition(oldActivation, newActivation, request.UserInfo)...)
	}
	if statusChanged {
		errors = append(errors, validateCatalogActivationStatusTransition(oldActivation, newActivation, request.UserInfo)...)
	}
	if specChanged && statusChanged {
		errors = append(errors, field.Forbidden(field.NewPath("status"), "spec publication and status acceptance must be separate API operations"))
	}
	if specChanged && len(errors) == 0 {
		if validator.PublicationVerifier == nil {
			errors = append(errors, field.Forbidden(field.NewPath("spec"), "catalog activation publication verification is unavailable"))
		} else if err := validator.PublicationVerifier.VerifyPublication(ctx, oldActivation, newActivation); err != nil {
			return nil, fmt.Errorf("verify live catalog activation publication: %w", err)
		}
	}
	return nil, invalidCatalogActivationIfAny(newActivation.Name, errors)
}

func (*PgShardCatalogActivationValidator) ValidateDelete(_ context.Context, _ *PgShardCatalogActivation) (admission.Warnings, error) {
	return nil, nil
}

func validateEmptyCatalogActivation(activation *PgShardCatalogActivation) field.ErrorList {
	metadataPath := field.NewPath("metadata")
	if activation == nil || len(activation.OwnerReferences) != 1 {
		return field.ErrorList{field.Invalid(metadataPath.Child("ownerReferences"), nil, "exactly one controlling PgShardCluster owner is required")}
	}
	owner := activation.OwnerReferences[0]
	cluster := &PgShardCluster{}
	cluster.Name = owner.Name
	cluster.Namespace = activation.Namespace
	cluster.UID = owner.UID
	expected := EmptyCatalogActivation(cluster)
	var errors field.ErrorList
	if owner.APIVersion != GroupVersion.String() || owner.Kind != "PgShardCluster" || owner.Controller == nil || !*owner.Controller || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion || owner.Name == "" || owner.UID == "" {
		errors = append(errors, field.Invalid(metadataPath.Child("ownerReferences"), activation.OwnerReferences, "the exact controlling PgShardCluster API identity is required"))
	}
	if !catalogActivationMetadataMatches(activation, expected) {
		errors = append(errors, field.Invalid(metadataPath, activation.ObjectMeta, "carrier metadata must have the fixed operator-owned shape"))
	}
	if activation.Spec.Request != nil || activation.Spec.RequestSHA256 != "" || activation.Status.Acceptance != nil {
		errors = append(errors, field.Forbidden(field.NewPath("spec"), "the operator may create only an empty catalog activation carrier"))
	}
	return errors
}

func validateCatalogActivationMetadataUpdate(oldActivation, newActivation *PgShardCatalogActivation) field.ErrorList {
	oldMetadata := oldActivation.ObjectMeta.DeepCopy()
	newMetadata := newActivation.ObjectMeta.DeepCopy()
	for _, metadata := range []*metav1.ObjectMeta{oldMetadata, newMetadata} {
		metadata.ResourceVersion = ""
		metadata.Generation = 0
		metadata.ManagedFields = nil
	}
	if reflect.DeepEqual(oldMetadata, newMetadata) {
		return nil
	}
	return field.ErrorList{field.Forbidden(field.NewPath("metadata"), "catalog activation carrier metadata is operator-owned and immutable")}
}

func validateCatalogActivationSpecTransition(oldActivation, newActivation *PgShardCatalogActivation, userInfo authenticationv1.UserInfo) field.ErrorList {
	path := field.NewPath("spec")
	if oldActivation.Spec.Request != nil || oldActivation.Spec.RequestSHA256 != "" || newActivation.Spec.Request == nil || newActivation.Spec.RequestSHA256 == "" {
		return field.ErrorList{field.Forbidden(path, "catalog activation request is set-once and immutable")}
	}
	if len(newActivation.OwnerReferences) != 1 {
		return field.ErrorList{field.Invalid(field.NewPath("metadata", "ownerReferences"), newActivation.OwnerReferences, "exactly one PgShardCluster owner is required")}
	}
	expectedUsername := "system:serviceaccount:" + newActivation.Namespace + ":" + activationClusterName(newActivation) + "-orchestrator"
	request := newActivation.Spec.Request
	if userInfo.Username != expectedUsername || !catalogActivationAdmissionPodMatches(userInfo.Extra, request.Dispatcher.PodName, request.Dispatcher.PodUID) {
		return field.ErrorList{field.Forbidden(path, "only the exact cluster orchestrator Pod identity may publish the request")}
	}
	digest, err := request.SHA256()
	if err != nil {
		return field.ErrorList{field.Invalid(path.Child("request"), request, err.Error())}
	}
	owner := newActivation.OwnerReferences[0]
	if request.Carrier.Name != newActivation.Name || request.Carrier.UID != newActivation.UID ||
		request.Cluster.Name != owner.Name || request.Cluster.Namespace != newActivation.Namespace || request.Cluster.UID != owner.UID ||
		request.Dispatcher.LeaseName != owner.Name+"-orch-lease" {
		return field.ErrorList{field.Invalid(path.Child("request"), request, "request does not bind the exact carrier, owner, and orchestrator Lease")}
	}
	if digest != newActivation.Spec.RequestSHA256 {
		return field.ErrorList{field.Invalid(path.Child("requestSHA256"), newActivation.Spec.RequestSHA256, "does not match the canonical request digest")}
	}
	return nil
}

func validateCatalogActivationStatusTransition(oldActivation, newActivation *PgShardCatalogActivation, userInfo authenticationv1.UserInfo) field.ErrorList {
	path := field.NewPath("status", "acceptance")
	if oldActivation.Status.Acceptance != nil || newActivation.Status.Acceptance == nil || newActivation.Spec.Request == nil {
		return field.ErrorList{field.Forbidden(path, "catalog activation acceptance is set-once and requires an immutable request")}
	}
	request := newActivation.Spec.Request
	expectedUsername := fmt.Sprintf("system:serviceaccount:%s:%s", newActivation.Namespace, PostgreSQLAgentServiceAccountName(request.Cluster.Name, request.Source.Shard))
	if userInfo.Username != expectedUsername || !catalogActivationAdmissionPodMatches(userInfo.Extra, request.Source.PodName, request.Source.PodUID) {
		return field.ErrorList{field.Forbidden(path, "only the exact source agent Pod identity may record durable acceptance")}
	}
	acceptance := newActivation.Status.Acceptance
	if acceptance.SchemaVersion != CatalogActivationAcceptanceVersion || acceptance.CarrierUID != newActivation.UID ||
		acceptance.RequestSHA256 != newActivation.Spec.RequestSHA256 || acceptance.TargetPodName != request.Source.PodName ||
		acceptance.TargetPodUID != request.Source.PodUID || acceptance.Persistence != CatalogActivationPersistenceFsync ||
		!validActivationDecimal(acceptance.PersistedAtUnixMS) {
		return field.ErrorList{field.Invalid(path, acceptance, "acceptance must bind the fsync-persisted exact request and target Pod")}
	}
	return nil
}

func catalogActivationAdmissionPodMatches(extra map[string]authenticationv1.ExtraValue, podName string, podUID types.UID) bool {
	names := extra["authentication.kubernetes.io/pod-name"]
	uids := extra["authentication.kubernetes.io/pod-uid"]
	return len(names) == 1 && names[0] == podName && len(uids) == 1 && uids[0] == string(podUID)
}

func activationClusterName(activation *PgShardCatalogActivation) string {
	if len(activation.OwnerReferences) != 1 {
		return ""
	}
	return activation.OwnerReferences[0].Name
}

func catalogActivationMetadataMatches(current, expected *PgShardCatalogActivation) bool {
	return current.Name == expected.Name && current.Namespace == expected.Namespace && current.GenerateName == "" &&
		reflect.DeepEqual(current.Labels, expected.Labels) && reflect.DeepEqual(current.Annotations, expected.Annotations) &&
		reflect.DeepEqual(current.OwnerReferences, expected.OwnerReferences) && len(current.Finalizers) == 0 && current.DeletionTimestamp == nil
}

func invalidCatalogActivationIfAny(name string, errors field.ErrorList) error {
	if len(errors) == 0 {
		return nil
	}
	return apierrors.NewInvalid(schema.GroupKind{Group: GroupVersion.Group, Kind: "PgShardCatalogActivation"}, name, errors)
}

// +kubebuilder:webhook:path=/validate-pgshard-io-v1alpha1-pgshardcatalogactivation,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups=pgshard.io,resources=pgshardcatalogactivations;pgshardcatalogactivations/status,verbs=create;update,versions=v1alpha1,name=vpgshardcatalogactivation.kb.io,admissionReviewVersions=v1,servicePort=9444
