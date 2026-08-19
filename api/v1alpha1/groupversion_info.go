// Package v1alpha1 contains the pgshard.io/v1alpha1 API types.
// +kubebuilder:object:generate=true
// +groupName=pgshard.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group and version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "pgshard.io", Version: "v1alpha1"}

	// SchemeBuilder registers the types below with a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds every type in this group-version to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme

	knownTypes []runtime.Object
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, knownTypes...)
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}

func register(objs ...runtime.Object) { knownTypes = append(knownTypes, objs...) }
