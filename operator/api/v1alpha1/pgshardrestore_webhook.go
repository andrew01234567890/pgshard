package v1alpha1

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// PgShardRestoreValidator enforces exact ordered topology comparison and
// immutable restore attempts without relying on high-cost CRD CEL traversal.
// +kubebuilder:object:generate=false
type PgShardRestoreValidator struct{}

func (*PgShardRestoreValidator) ValidateCreate(_ context.Context, restore *PgShardRestore) (admission.Warnings, error) {
	return nil, invalidRestoreIfAny(restore.Name, validateRequestedRestoreTopology(restore))
}

func (*PgShardRestoreValidator) ValidateUpdate(_ context.Context, oldRestore, newRestore *PgShardRestore) (admission.Warnings, error) {
	errors := validateRequestedRestoreTopology(newRestore)
	if !apiequality.Semantic.DeepEqual(oldRestore.Spec, newRestore.Spec) {
		errors = append(errors, field.Invalid(field.NewPath("spec"), newRestore.Spec, "restore specification is immutable"))
	}
	return nil, invalidRestoreIfAny(newRestore.Name, errors)
}

func (*PgShardRestoreValidator) ValidateDelete(_ context.Context, _ *PgShardRestore) (admission.Warnings, error) {
	return nil, nil
}

func validateRequestedRestoreTopology(restore *PgShardRestore) field.ErrorList {
	if restore.Spec.DestinationTopology == nil || apiequality.Semantic.DeepEqual(*restore.Spec.DestinationTopology, restore.Spec.Manifest.Topology) {
		return nil
	}
	return field.ErrorList{field.Invalid(
		field.NewPath("spec", "destinationTopology"),
		restore.Spec.DestinationTopology,
		"RestoreTopologyMismatch: requested destination topology must exactly match the backup manifest, including ordered ordinals and range boundaries",
	)}
}

func invalidRestoreIfAny(name string, errors field.ErrorList) error {
	if len(errors) == 0 {
		return nil
	}
	return apierrors.NewInvalid(schema.GroupKind{Group: GroupVersion.Group, Kind: "PgShardRestore"}, name, errors)
}

// +kubebuilder:webhook:path=/validate-pgshard-io-v1alpha1-pgshardrestore,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups=pgshard.io,resources=pgshardrestores,verbs=create;update,versions=v1alpha1,name=vpgshardrestore.kb.io,admissionReviewVersions=v1,servicePort=9444
