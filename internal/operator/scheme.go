// Package operator holds the Kubernetes operator's runtime wiring.
package operator

import (
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// NewScheme returns a scheme with the core Kubernetes and pgshard.io types registered.
func NewScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, err
	}
	if err := pgshardv1alpha1.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}
