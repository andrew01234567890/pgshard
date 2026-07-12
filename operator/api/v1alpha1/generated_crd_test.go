package v1alpha1

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestGeneratedCRDPassesAPIServerValidation(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../config/crd/bases/pgshard.io_pgshardclusters.yaml")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := utilyaml.ToJSON(contents)
	if err != nil {
		t.Fatal(err)
	}
	external := &apiextensionsv1.CustomResourceDefinition{}
	if err := json.Unmarshal(encoded, external); err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := apiextensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	internal := &apiextensions.CustomResourceDefinition{}
	if err := scheme.Convert(external, internal, nil); err != nil {
		t.Fatal(err)
	}
	// The CRD storage strategy populates this status field after creation. The
	// static schema/CEL validator expects that API-server-owned value to exist.
	internal.Status.StoredVersions = []string{"v1alpha1"}
	if errors := apiextensionsvalidation.ValidateCustomResourceDefinition(context.Background(), internal); len(errors) != 0 {
		t.Fatalf("generated CRD is rejected by kube-apiserver validation: %v", errors.ToAggregate())
	}
}
